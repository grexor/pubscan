aws s3 sync 's3://openalex/data/works' 'openalex-snapshot/data/works' \
  --no-sign-request \
  --exact-timestamps \
  --delete

aws s3 sync 's3://openalex/data/authors' 'openalex-snapshot/data/authors' \
  --no-sign-request \
  --exact-timestamps \
  --delete

aws s3 sync 's3://openalex/data/institutions' 'openalex-snapshot/data/institutions' \
  --no-sign-request \
  --exact-timestamps \
  --delete
