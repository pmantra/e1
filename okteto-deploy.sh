#!/bin/bash

export ENVIRONMENT="multiverse"

export TAG="${IMAGE_TAG:-$OKTETO_GIT_COMMIT}"
echo "======== Using image tag -- $TAG"

if [[ -z "${ARTIFACT_REGISTRY_SERVICE_ACCOUNT_KEY}" ]]; then
  REGISTRY_CREDENTIALS="$(gcloud auth print-access-token)"
else
  REGISTRY_CREDENTIALS="${ARTIFACT_REGISTRY_SERVICE_ACCOUNT_KEY}"
fi
echo "${REGISTRY_CREDENTIALS}" | helm registry login us-east1-docker.pkg.dev -u _json_key --password-stdin

helm dependency update ./chart
helm upgrade --install --debug eligibility ./chart -f ./chart/values.yaml -f ./chart/values.multiverse.yaml \
  --set ci.commitSha="${IMAGE_TAG}" \
  --set commitSha="${IMAGE_TAG}"  \
  --set AppVersion="${IMAGE_TAG}"  \
  --set api.deploymentV1.containerSchemas.api.imageTag="${IMAGE_TAG}" \
  --set jobs.seedDatabase.jobV1.containerSchemas.primary.imageTag="${IMAGE_TAG}" \
  --set jobs.migrations.jobV1.containerSchemas.eligibility-migrations.imageTag="${IMAGE_TAG}"
