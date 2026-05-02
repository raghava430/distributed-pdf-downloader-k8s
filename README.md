# Distributed PDF Downloader on Kubernetes

This project runs a Dockerized Python PDF downloader as a Kubernetes Job. It reads URLs from an Excel file, splits the workload across multiple pods, downloads PDF attachments, and uploads the results to Oracle Cloud Infrastructure Object Storage.

## Project Overview

The application is designed for distributed PDF downloading using a lightweight Kubernetes setup. A K3s cluster can run multiple worker pods in parallel, where each pod processes a separate slice of the input URL list.

## Architecture

```text
Excel URL File
      |
      v
Kubernetes ConfigMap
      |
      v
Indexed Kubernetes Job
      |
      v
Parallel PDF Downloader Pods
      |
      v
OCI Object Storage Bucket
```

## Repository Structure

```text
.
├── app/
│   └── attachment_downloader_docker.py
├── Docker/
│   ├── Dockerfile
│   └── requirements.txt
├── k8s/
│   └── pdf-grabber-job.yaml
├── input/
│   └── .gitkeep
├── downloads/
│   └── .gitkeep
├── docs/
│   ├── logs/      # ignored local logs
│   └── setup/     # ignored private setup notes
├── .gitignore
└── README.md
```

## What This Project Does

- Reads PDF source URLs from an Excel file.
- Splits the workload across Kubernetes pods using an Indexed Job.
- Uses Selenium with headless Chrome to access pages and detect PDF attachments.
- Downloads PDF files inside the container runtime.
- Uploads downloaded PDFs to OCI Object Storage.
- Supports local checkpointing and optional PostgreSQL-based progress tracking.

## Prerequisites

- Docker
- Docker Hub account
- Kubernetes or K3s cluster
- Oracle Cloud Infrastructure account
- OCI Object Storage bucket
- Excel file containing URLs
- Instance Principal permissions for OCI Object Storage access

## Build Docker Image

From the repository root, run:

```bash
docker build -f Docker/Dockerfile -t pdf-grabber:v1 .
```

Tag the image:

```bash
docker tag pdf-grabber:v1 your-dockerhub-username/pdf-grabber:v1
```

Push the image:

```bash
docker push your-dockerhub-username/pdf-grabber:v1
```

## Prepare Input File

The input Excel file should be named:

```text
urls.xlsx
```

The first column should contain the URLs to process.

Do not commit real input files to GitHub. The `input/` folder is kept only as a placeholder.

Create a ConfigMap from the Excel file:

```bash
kubectl create namespace pdf-grabber

kubectl create configmap pdf-grabber-config \
  --from-file=urls.xlsx \
  -n pdf-grabber
```

## Configure Kubernetes Job

Before applying the job, update the image name in `k8s/pdf-grabber-job.yaml`:

```yaml
image: your-dockerhub-username/pdf-grabber:v1
```

Also replace the OCI placeholder values:

```yaml
- name: OCI_NAMESPACE
  value: "replace-with-your-oci-namespace"
- name: OCI_BUCKET
  value: "replace-with-your-bucket-name"
```

## Run the Kubernetes Job

Apply the manifest:

```bash
kubectl apply -f k8s/pdf-grabber-job.yaml
```

Check pods:

```bash
kubectl get pods -n pdf-grabber
```

Follow logs:

```bash
kubectl logs -l job-name=pdf-grabber-job -n pdf-grabber -f
```

Check job status:

```bash
kubectl get jobs -n pdf-grabber
```

## Environment Variables

| Variable | Description |
|---|---|
| `POD_INDEX` | Pod index assigned by Kubernetes Indexed Job |
| `POD_COUNT` | Total number of worker pods |
| `TOTAL_URLS` | Number of URLs to process |
| `PARALLEL_DOWNLOADS` | Number of parallel downloads per pod |
| `OCI_NAMESPACE` | OCI Object Storage namespace |
| `OCI_BUCKET` | OCI Object Storage bucket name |
| `OCI_PREFIX` | Prefix/path used when uploading files |

## Git Ignore Policy

This repository intentionally excludes private setup notes, raw logs, Excel input files, downloaded PDFs, and local environment files.

Ignored local-only paths include:

```text
docs/setup/
docs/logs/
input/*.xlsx
downloads/*
.env
venv/
```

## Future Improvements

- Add automated K3s setup scripts.
- Add Helm chart support.
- Add CI workflow for Docker image builds.
- Add sample input file format.
- Add architecture diagram under `docs/`.
