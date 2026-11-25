# S3 permissions

litestream-rs works under the policy Litestream's S3 guide publishes: `s3:GetBucketLocation` and
`s3:ListBucket` on the bucket, `s3:PutObject`, `s3:DeleteObject` and `s3:GetObject` on its objects.
Two more actions are optional; without them nothing fails, and a start-up clean-up is skipped.

| Request litestream-rs sends | IAM action | Needed | Without it |
|---|---|---|---|
| ListObjectsV2 | `s3:ListBucket` | Yes | The start is refused: nothing can be judged without the listing, so `hold-start` keeps the application's writes held |
| GetObject, ranged GET and HEAD of an object | `s3:GetObject` | Yes | Restore, the start-up check of the newest files and compaction fail |
| PutObject, CreateMultipartUpload, UploadPart, CompleteMultipartUpload (also with `If-None-Match`) | `s3:PutObject` | Yes | Nothing is uploaded |
| DeleteObjects | `s3:DeleteObject` | Yes | Retention and compaction cannot delete; the bucket grows |
| ListMultipartUploads | `s3:ListBucketMultipartUploads` | No | The start-up sweep of uploads a killed process left open is skipped, once, with a warning and `lsr_scratch_sweeps_denied_total` set to 1 |
| AbortMultipartUpload | `s3:AbortMultipartUpload` | No | An upload that failed or was dropped stays open until the sweep or a bucket rule removes it; litestream-rs ignores the refusal |
| GetBucketLocation | `s3:GetBucketLocation` | No | Never sent. Go's guide lists it; litestream-rs reads the region from its configuration |

Go sweeps nothing at start, so its policy has neither optional action. An open multipart upload is
billed until it is aborted. Where the optional actions are not granted, add a bucket lifecycle rule
with `AbortIncompleteMultipartUpload` (a few days) to remove what a crash leaves.

`just compat-s3` runs `lsr replicate` and a restore as a user whose policy is exactly the five actions
of the guide (`over_s3_lsr_runs_on_the_policy_litestreams_guide_publishes`).
