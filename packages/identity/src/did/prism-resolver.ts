import { DidCache } from '../types'
import { BaseResolver } from './base-resolver'
import { timed } from './util'

export class DidPrismResolver extends BaseResolver {
  constructor(
    public prismUrl: string,
    public timeout: number,
    public cache?: DidCache,
  ) {
    super(cache)
  }

  async resolveNoCheck(did: string): Promise<unknown> {
    // Extract account ID from did:prism:abc123 -> abc123
    console.log('use prism did resolver 🫂')
    const accountId = did.split(':')[2]
    return timed(this.timeout, async (signal) => {
      const res = await fetch(`${this.prismUrl}/get-did-document`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Accept: 'application/json',
        },
        body: JSON.stringify({ id: accountId }),
        signal,
      })

      if (!res.ok) {
        throw Object.assign(new Error(res.statusText), { status: res.status })
      }

      const response = await res.json()
      console.log(response)
      return response.did_document
    })
  }
}
