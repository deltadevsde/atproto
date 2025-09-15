/* eslint-env node */
/* eslint-disable import/order */

'use strict'

const dd = require('dd-trace')

dd.tracer
  .init()
  .use('http2', {
    client: true, // calls into dataplane
    server: false,
  })
  .use('express', {
    hooks: {
      request: (span, req) => {
        maintainXrpcResource(span, req)
      },
    },
  })

// modify tracer in order to track calls to dataplane as a service with proper resource names
const DATAPLANE_PREFIX = '/bsky.Service/'
const origStartSpan = dd.tracer._tracer.startSpan
dd.tracer._tracer.startSpan = function (name, options) {
  if (
    name !== 'http.request' ||
    options?.tags?.component !== 'http2' ||
    !options?.tags?.['http.url']
  ) {
    return origStartSpan.call(this, name, options)
  }
  const uri = new URL(options.tags['http.url'])
  if (!uri.pathname.startsWith(DATAPLANE_PREFIX)) {
    return origStartSpan.call(this, name, options)
  }
  options.tags['service.name'] = 'dataplane-bsky'
  options.tags['resource.name'] = uri.pathname.slice(DATAPLANE_PREFIX.length)
  return origStartSpan.call(this, name, options)
}

// Tracer code above must come before anything else
const assert = require('node:assert')
const cluster = require('node:cluster')
const path = require('node:path')

const {
  BskyAppView,
  ServerConfig,
  Database,
  DataPlaneServer,
  MockBsync,
  RepoSubscription,
} = require('@atproto/bsky')
const { Secp256k1Keypair } = require('@atproto/crypto')

const main = async () => {
  const env = getEnv()

  // If external deps are not provided, start embedded dataplane + bsync.
  const useEmbeddedDeps = shouldEmbedDeps()

  let db, dataplane, bsync, sub
  let overrides = {}

  if (useEmbeddedDeps) {
    const { dbUrl, dbSchema, repoProvider, plcUrl } = getEmbeddedEnv()
    assert(
      dbUrl,
      'must set DB_POSTGRES_URL (or BSKY_DB_POSTGRES_URL) for embedded deps',
    )

    db = new Database({ url: dbUrl, schema: dbSchema, poolSize: 10 })

    // Run migrations on embedded DB to ensure schema exists
    const migrationDb = new Database({ url: dbUrl, schema: dbSchema })
    await migrationDb.migrateToLatestOrThrow()
    await migrationDb.close()

    // Start dataplane and mock bsync on ephemeral ports
    dataplane = await DataPlaneServer.create(
      db,
      3001,
      plcUrl,
      'http://host.docker.internal:41997',
    )
    const dpAddr = dataplane.server.address()
    const dpPort =
      typeof dpAddr === 'object' && dpAddr ? dpAddr.port : undefined
    assert(dpPort, 'failed to determine dataplane port')

    bsync = await MockBsync.create(db, 0)
    const bsAddr = bsync.server.address()
    const bsPort =
      typeof bsAddr === 'object' && bsAddr ? bsAddr.port : undefined
    assert(bsPort, 'failed to determine bsync port')

    overrides = {
      dataplaneUrls: [`http://127.0.0.1:${dpPort}`],
      dataplaneHttpVersion: '1.1',
      bsyncUrl: `http://127.0.0.1:${bsPort}`,
      bsyncHttpVersion: '1.1',
    }

    // Optional: start firehose subscription if provider specified
    if (repoProvider) {
      sub = new RepoSubscription({
        service: repoProvider,
        db,
        idResolver: dataplane.idResolver,
      })
      sub.start()
    }
  }

  const config = ServerConfig.readEnv(overrides)
  assert(env.serviceSigningKey, 'must set BSKY_SERVICE_SIGNING_KEY')
  const signingKey = await Secp256k1Keypair.import(env.serviceSigningKey)
  const bsky = BskyAppView.create({ config, signingKey })
  await bsky.start()

  // Graceful shutdown (see also https://aws.amazon.com/blogs/containers/graceful-shutdowns-with-ecs/)
  const shutdown = async () => {
    await bsky.destroy()
    if (sub?.destroy) await sub.destroy()
    if (bsync?.destroy) await bsync.destroy()
    if (dataplane?.destroy) await dataplane.destroy()
    if (db?.close) await db.close()
  }
  process.on('SIGTERM', shutdown)
  process.on('disconnect', shutdown) // when clustering
}

const getEnv = () => ({
  serviceSigningKey: process.env.BSKY_SERVICE_SIGNING_KEY || undefined,
})

const shouldEmbedDeps = () => {
  // Allow explicit opt-out/opt-in
  if (process.env.BSKY_EMBED_DEPS === 'true') return true
  if (process.env.BSKY_EMBED_DEPS === 'false') return false
  // If not explicitly configured with external services, embed them
  const hasDataplane = !!(
    process.env.BSKY_DATAPLANE_URLS || process.env.BSKY_DATAPLANE_URL
  )
  const hasBsync = !!process.env.BSKY_BSYNC_URL
  return !(hasDataplane && hasBsync)
}

const getEmbeddedEnv = () => {
  return {
    dbUrl:
      process.env.BSKY_DB_POSTGRES_URL ||
      process.env.DB_POSTGRES_URL ||
      undefined,
    dbSchema:
      process.env.BSKY_DB_POSTGRES_SCHEMA ||
      process.env.DB_POSTGRES_SCHEMA ||
      undefined,
    repoProvider:
      process.env.BSKY_REPO_PROVIDER || process.env.REPO_PROVIDER || undefined,
    plcUrl: process.env.BSKY_DID_PLC_URL || 'http://localhost:2582',
  }
}

const maybeParseInt = (str) => {
  if (!str) return
  const int = parseInt(str, 10)
  if (isNaN(int)) return
  return int
}

const maintainXrpcResource = (span, req) => {
  // Show actual xrpc method as resource rather than the route pattern
  if (span && req.originalUrl?.startsWith('/xrpc/')) {
    span.setTag(
      'resource.name',
      [
        req.method,
        path.posix.join(req.baseUrl || '', req.path || '', '/').slice(0, -1), // Ensures no trailing slash
      ]
        .filter(Boolean)
        .join(' '),
    )
  }
}

const workerCount = maybeParseInt(process.env.CLUSTER_WORKER_COUNT)

if (workerCount) {
  if (cluster.isPrimary) {
    console.log(`primary ${process.pid} is running`)
    const workers = new Set()
    for (let i = 0; i < workerCount; ++i) {
      workers.add(cluster.fork())
    }
    let teardown = false
    cluster.on('exit', (worker) => {
      workers.delete(worker)
      if (!teardown) {
        workers.add(cluster.fork()) // restart on crash
      }
    })
    process.on('SIGTERM', () => {
      teardown = true
      console.log('disconnecting workers')
      workers.forEach((w) => w.disconnect())
    })
  } else {
    console.log(`worker ${process.pid} is running`)
    main()
  }
} else {
  main() // non-clustering
}
