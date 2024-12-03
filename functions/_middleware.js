// functions/_middleware.js
import { AwsClient } from './aws4fetch.js'

const UNSIGNABLE_HEADERS = [
  'x-forwarded-proto',
  'x-real-ip',
  'accept-encoding'
]

const HTTPS_PROTOCOL = 'https:'
const HTTPS_PORT = '443'
const RANGE_RETRY_ATTEMPTS = 3

function filterHeaders(headers, env) {
  return new Headers(
    Array.from(headers.entries()).filter(
      (pair) =>
        !UNSIGNABLE_HEADERS.includes(pair[0]) &&
        !pair[0].startsWith('cf-') &&
        !('ALLOWED_HEADERS' in env && !env.ALLOWED_HEADERS.includes(pair[0]))
    )
  )
}

export async function onRequest({ request, env }) {
  if (!['GET', 'HEAD'].includes(request.method)) {
    return new Response(null, {
      status: 405,
      statusText: 'Method Not Allowed',
    })
  }

  const url = new URL(request.url)
  url.protocol = HTTPS_PROTOCOL
  url.port = HTTPS_PORT
  
  let path = url.pathname.replace(/^\//, '')
  path = path.replace(/\/$/, '')
  const pathSegments = path.split('/')

  if (env.ALLOW_LIST_BUCKET !== 'true') {
    if (
      (env.BUCKET_NAME === '$path' && pathSegments.length < 2) ||
      (env.BUCKET_NAME !== '$path' && path.length === 0)
    ) {
      return new Response(null, {
        status: 404,
        statusText: 'Not Found',
      })
    }
  }

  switch (env.BUCKET_NAME) {
    case '$path':
      url.hostname = env.B2_ENDPOINT
      break
    case '$host':
      url.hostname = url.hostname.split('.')[0] + '.' + env.B2_ENDPOINT
      break
    default:
      url.hostname = env.BUCKET_NAME + '.' + env.B2_ENDPOINT
      break
  }

  const headers = filterHeaders(request.headers, env)
  const endpointRegex = /^s3\.([a-zA-Z0-9-]+)\.backblazeb2\.com$/
  const [, aws_region] = env.B2_ENDPOINT.match(endpointRegex)

  const client = new AwsClient({
    accesskeyID: env.B2_APPLICATION_KEY_ID,
    secretAccessKey: env.B2_APPLICATION_KEY,
    service: 's3',
    region: aws_region,
  })

  const signedRequest = await client.sign(url.toString(), {
    method: request.method,
    headers,
  })

  if (signedRequest.headers.has('range')) {
    let attempts = RANGE_RETRY_ATTEMPTS
    let response
    do {
      let controller = new AbortController()
      response = await fetch(signedRequest.url, {
        method: signedRequest.method,
        headers: signedRequest.headers,
        signal: controller.signal,
      })
      if (response.headers.has('content-range')) {
        if (attempts < RANGE_RETRY_ATTEMPTS) {
          console.log(`Retry for ${signedRequest.url} succeeded - response has content-range header`)
        }
        break
      } else if (response.ok) {
        attempts -= 1
        console.error(
          `Range header in request for ${signedRequest.url} but no content-range header in response. Will retry ${attempts} more times`
        )
        if (attempts > 0) {
          controller.abort()
        }
      } else {
        break
      }
    } while (attempts > 0)
    if (attempts <= 0) {
      console.error(
        `Tried range request for ${signedRequest.url} ${RANGE_RETRY_ATTEMPTS} times, but no content-range in response.`
      )
    }
    return response
  }

  return fetch(signedRequest)
}