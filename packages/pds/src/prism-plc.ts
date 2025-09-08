import * as plc from '@did-plc/lib'

export async function createOp(input: any) {
  const result = await plc.createOp(input)

  const prismDid = result.did.replace('did:plc:', 'did:prism:')

  return {
    ...result,
    did: prismDid,
  }
}

export * from '@did-plc/lib'
