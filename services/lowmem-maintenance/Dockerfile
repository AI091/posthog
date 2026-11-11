FROM python:3.12-slim AS queues
WORKDIR /src
COPY posthog/celery_queues.py posthog/celery_queues.py
COPY services/lowmem-maintenance/export_queues.py services/lowmem-maintenance/export_queues.py
RUN python services/lowmem-maintenance/export_queues.py > /queues.json

FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY services/lowmem-maintenance/go.mod services/lowmem-maintenance/go.sum ./
RUN go mod download
COPY services/lowmem-maintenance/*.go ./
RUN CGO_ENABLED=0 go build -trimpath -o /maintenance .

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /maintenance /app/maintenance
COPY --from=queues /queues.json /app/queues.json
ENTRYPOINT ["/app/maintenance"]
