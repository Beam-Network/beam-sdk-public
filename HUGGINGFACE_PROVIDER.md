# Hugging Face Hub provider

How the `huggingface` provider moves a file between the Hugging Face Hub and any
other Beam provider, and why its constraints are what they are.

Everything below is taken from `huggingface_hub` v1.29.0 — the published
documentation and the library source it ships — and the behavioural claims were
then checked against the live `huggingface.co` API. Source references are given so
each claim can be re-checked against a specific version.

`sdks/typescript/test/huggingface.live.test.mjs` re-runs the live checks; it skips
unless `HF_TOKEN` is set, so the default suite stays offline. The upload check
commits to a real repo and needs `HF_LIVE_WRITE_REPO` as a second opt-in:

```bash
HF_TOKEN=hf_... \
HF_LIVE_BUCKET=owner/bucket HF_LIVE_BUCKET_PATH=file.bin \
HF_LIVE_WRITE_REPO=owner/dataset \
  node --test test/huggingface.live.test.mjs
```

## The shape it fits

Beam providers are *configs plus presigners*. The SDK holds the credentials,
turns each source into a plain HTTP URL and each destination into signed upload
targets, and hands BeamCore only prepared HTTP sources, prepared destinations,
and signed chunk routes. Workers do HTTP GET and PUT and never see a credential.

The Hub fits that model, with one asymmetry: for a source the SDK mints the URL,
while for a destination the *Hub* mints them and the SDK passes them through.

## Config

```
provider: "huggingface"
repo_id:  "org/name"
path:     path of the file inside the repo or bucket
repo_type: "model" | "dataset" | "space" | "kernel" | "bucket"  (default "model")
revision:  branch, tag or commit             (default "main"; buckets ignore it)
token:     Hub access token
endpoint:  Hub endpoint                                (default https://huggingface.co)

# destination only
commit_message, commit_description, create_pr
allow_source_rehash
```

## Reading: the resolve URL and its redirect

`hf_hub_url` builds
`{endpoint}/{prefix}{repo_id}/resolve/{revision}/{path}`, where `prefix` comes
from `REPO_TYPES_URL_PREFIXES` — `datasets/`, `spaces/`, `kernels/`, and nothing
at all for a model (`file_download.py:201-286`, `constants.py:121-125`).

**Buckets** are a fifth repo type with a different shape: they are unversioned, so
there is no revision segment, and the Hub escapes the whole key as one component
— `{endpoint}/buckets/{bucket_id}/resolve/{quoted_path}`
(`HfApi.get_bucket_file_metadata`).

A HEAD on that URL with `Authorization: Bearer <token>` and
`Accept-Encoding: identity` returns (`file_download.py:1576-1650`):

| Header | Meaning |
| --- | --- |
| `Location` | where the bytes actually live |
| `X-Linked-Size` | the real object size (`Content-Length` on a redirect is the redirect body) |
| `X-Linked-Etag` | the object's **sha256** for an LFS blob, its git sha1 otherwise |
| `X-Repo-Commit` | the commit the revision resolved to |

For large-file content that `Location` is a **presigned CDN URL that carries no
credential** — `_get_metadata_or_catch_error` deliberately drops the auth header
once it sees a cross-host redirect (`file_download.py:1776-1781`). That URL is the
exact analogue of an S3 presigned GET: it supports `Range`, and it is the only
form of the URL Beam will hand to BeamCore and the workers.

**Beam requires that redirect.** If the Hub answers inline instead, the SDK
rejects the source and says why — a small regular (non-LFS) blob is served from
the Hub's own domain (verified: `README.md` on a public dataset answers `307`
with a *relative* `Location` of `/api/resolve-cache/…`, which is the same host).
Supporting it would mean forwarding the Hub token to BeamCore and the workers, so
it is rejected rather than silently permitted.

**Xet-backed storage is not a barrier**, despite what a first read of
`huggingface_hub` suggests. `_get_metadata_or_catch_error` leaves
`url_to_download` on the resolve URL when `xet_file_data` is present
(`file_download.py:1776-1781`), but that is the library *preferring* its Xet CAS
client — the Hub still returns the CDN redirect. Verified live: a Xet-backed
dataset file (`x-xet-hash` present) and a Xet-only bucket both `302` to
`us.aws.cdn.hf.co`, and both serve ranged reads with no credential.

Verified live against `huggingface.co` on 2026-09-01:

| Check | Result |
| --- | --- |
| Xet-backed dataset parquet, HEAD with token | `302` → `us.aws.cdn.hf.co`, `x-linked-size` correct |
| That CDN URL, ranged GET with **no** `Authorization` | `206`, correct `content-range`, correct bytes |
| `x-linked-etag` vs sha256 of the full 20 MB object | identical |
| Small regular `README.md` | `307` to a same-host relative path → rejected |
| **Private** bucket, HEAD with no token | `401` |
| **Private** bucket, HEAD with token | `302` → signed CDN URL |
| That URL, ranged GET with **no** `Authorization` | `206`, correct bytes |
| Bucket LFS batch endpoint | `404` — buckets are source-only |

And the write path, driven end to end through the SDK's own functions with the
per-part PUTs standing in for Beam's workers — a 100 MB private bucket file into
a dataset repo:

| Step | Result |
| --- | --- |
| preupload | `uploadMode: "lfs"`, `shouldIgnore: false` |
| LFS batch | multipart, `chunk_size` **16,000,000**, 7 presigned part URLs |
| 7 part PUTs from source ranges | all `200` |
| **S3-returned ETag vs locally computed MD5** | **identical on every part** |
| completion `{oid, parts}` | accepted |
| `actions.verify` | not offered for this repo — the step is genuinely optional |
| NDJSON commit | `578bb6a900ca…` |
| read back: size and sha256 vs source | **byte-identical** |

That last table is the evidence for the ETag design below: the Hub's LFS bucket
returns plain MD5 ETags, so computing them locally is sound. The completion is
still the safety net — S3 validates every ETag and rejects the whole upload on a
mismatch, so a future change there fails loudly rather than corrupting data.

The CDN URL is a CloudFront signed URL (`Expires`, `Key-Pair-Id`, `Policy`,
`Signature`) with a **1-hour** lifetime — the same as Beam's default `expiresIn`.
Route re-signing repeats the same HEAD to mint a fresh one, and the token never
leaves the SDK process.

## Writing: preupload, LFS batch, completion, commit

Four plain HTTP steps, no Hub SDK required.

**1. Preupload** — `POST {endpoint}/api/{repo_type}s/{repo_id}/preupload/{revision}`
with `{files:[{path, sample, size}]}`, where `sample` is the base64 of the first
512 bytes. The answer carries `uploadMode: "lfs" | "regular"`, `shouldIgnore`,
and `oid` (`_commit_api.py:735-767`). Beam requires `lfs`; a `regular` result
means the path is not marked as LFS in `.gitattributes` and is rejected with that
reason.

**2. LFS batch** — `POST {endpoint}/{prefix}{repo_id}.git/info/lfs/objects/batch`
with (`lfs.py:129-221`):

```json
{
  "operation": "upload",
  "transfers": ["basic", "multipart"],
  "hash_algo": "sha256",
  "ref": { "name": "<revision>" },
  "objects": [{ "oid": "<sha256 hex>", "size": 1234 }]
}
```

The answer gives, per object, `actions.upload.href` plus an optional
`actions.upload.header` holding `chunk_size` and one zero-padded key per part
(`"00001"`, `"00002"`, …) mapping to a **presigned S3 PUT URL**, and an optional
`actions.verify.href`. `_get_sorted_parts_urls` asserts
`len(parts) == ceil(size / chunk_size)` (`lfs.py:394-408`). No `actions` at all
means the Hub already stores this content and only the commit is left to do.

Those part URLs are what Beam's workers PUT to. Beam does not sign them.

**3. Completion** — the parts are closed by `POST`ing to `actions.upload.href`
(the completion endpoint, not an S3 URL) with
`{oid, parts:[{partNumber, etag}]}` (`lfs.py:371-392, 411-424`), then optionally
`POST actions.verify.href` with `{oid, size}`.

**4. Commit** — `POST {endpoint}/api/{repo_type}s/{repo_id}/commit/{revision}` as
NDJSON: a header line, then one `lfsFile` line
(`_commit_api.py:902-960`):

```
{"key":"header","value":{"summary":"…","description":"…"}}
{"key":"lfsFile","value":{"path":"…","algo":"sha256","oid":"…","size":1234}}
```

Until this lands the bytes exist but the file does not appear in the repo.

## How that maps onto a Beam transfer

1. **Hash pass.** The Hub will not issue upload URLs without the object's
   sha256, so the SDK must know it before the plan exists. When the source is
   itself a Hub file the sha256 is free — it is the `X-Linked-Etag`. Otherwise
   the SDK streams the source once to compute it, which only happens when
   `allow_source_rehash` is set.
2. **Preupload and LFS batch**, which yield the Hub's `chunk_size` and the part
   URLs.
3. **Prepare** is then requested at *that* `chunk_size`. BeamCore may normalise a
   requested size (`chunking.ts:105-136`), so the SDK asserts that the returned
   `plan_descriptor.chunk_size` and the source's `chunk_count` match what the Hub
   issued, and fails loudly rather than mis-mapping parts.
4. **Route signing.** Chunk N carries the Hub's URL for part N + 1 as its
   `dest_url`. BeamCore does not treat `huggingface` as an S3 multipart
   destination, so these routes carry no `part_number`, `upload_id`,
   `complete_url`, `abort_url`, `list_page_url`, `final_head_url` or
   `multipart_group_id` — it rejects those as unexpected multipart signals.
5. **Finalize**, once the transfer reaches terminal `completed`: the completion
   POST, the optional verify, then the commit. This runs after the transfer, never
   inside it. `waitForTransfer` / `wait_complete` does it for you; otherwise call
   `finalizeHuggingFaceUploads` / `finalize_huggingface_uploads` /
   `FinalizeHuggingFaceUploads` / `finalize_huggingface_uploads` yourself.

## Constraints, and where each comes from

| Constraint | Why |
| --- | --- |
| Sources must be large-file (LFS or Xet) content | Only those get a credential-free CDN redirect; a small regular blob is served inline and would mean forwarding the token to BeamCore and the workers |
| Buckets are source-only | They expose no LFS batch endpoint (verified: `404`); their only write path is the Hub's Xet CAS client, which cannot be handed to workers as URLs. Use `hf sync` to write a bucket |
| A destination needs the source sha256 before any byte moves | The LFS batch endpoint will not issue upload URLs without it |
| Exactly one source per Hugging Face destination | The Hub dictates the part size and a Beam plan carries a single global chunk size |
| Part ETags are computed locally as per-part MD5 | The ETags the workers get back stay inside BeamCore's finalization and never reach the SDK. Verified live: S3's returned ETag matched the locally computed MD5 on every part of a 7-part upload. If that ever changes, S3 rejects the completion outright — a loud failure, not silent corruption |
| The upload is not visible until finalize runs | The commit is a separate Hub call; the parts alone are not a file |

## Per-SDK entry points

| SDK | Source and destination | Finalize |
| --- | --- | --- |
| TypeScript | `HuggingFaceProviderConfig.create({...})` in `prepareProviderTransfer` | `finalizeHuggingFaceUploads(transferId)` |
| Python | `HuggingFaceProviderSource` / `HuggingFaceProviderDestination` in `prepare_provider_transfer` | `finalize_huggingface_uploads(transfer_id)` |
| Go | `HuggingFaceProviderSource` / `HuggingFaceProviderDestination` in `PrepareProviderTransfer` | `FinalizeHuggingFaceUploads(ctx, transferID)` |
| Rust | `prepare_huggingface_provider_transfer` | `finalize_huggingface_uploads(transfer_id)` |

The Python CLI exposes the same flow over the Hub's own URI grammar,
`hf://[<type>/]<org>/<repo>[@<revision>]/<path>` (`utils/_hf_uris.py`):

```bash
beam-send hf-transfer \
  hf://datasets/org/dataset@main/data/train.parquet \
  hf://datasets/org/mirror@main/data/train.parquet
```
