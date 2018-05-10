# BigQuery Data Transfer Service -  Integration SDK

This SDK provides a framework to process BigQuery Data Transfer Service Transfer Run requests.

## Before you begin

You will need to do the following:

1. Install the [gcloud SDK](https://cloud.google.com/sdk/downloads#interactive)

2. Enable the following APIs in the [Google Developers Console](https://console.developers.google.com/project/_/apiui/apiview/pubsub/overview).
    * BigQuery Data Transfer API
    * Cloud Pub/Sub API

    ```
    gcloud services enable bigquerydatatransfer.googleapis.com
    gcloud services enable pubsub.googleapis.com
    ```

3. Create an IAM Service Account and download credentials for use with your source.  Running these commands
    1. Creates a new Service Account named `bq-dts-[SOURCE]@[PROJECT_ID].iam.gserviceaccount.com`
    2. Grants roles `bigquery.admin`, `pubsub.subscriber`, `storage.objectAdmin`
    3. Downloads a Service-Account key called `.gcp-service-account.json`

    ```
    SOURCE="example-calendar" # REPLACE THIS VARIABLE
    PROJECT_ID=$(gcloud config get-value core/project)
    PARTNER_SA_NAME="bq-dts-${SOURCE}"
    PARTNER_SA_EMAIL="${PARTNER_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

    # Creating a Service Account
    gcloud iam service-accounts create ${PARTNER_SA_NAME} --display-name ${PARTNER_SA_NAME}

     # Granting Service Account required roles
    gcloud projects add-iam-policy-binding ${PROJECT_ID} --member="serviceAccount:${PARTNER_SA_EMAIL}" --role='roles/bigquery.admin'
    gcloud projects add-iam-policy-binding ${PROJECT_ID} --member="serviceAccount:${PARTNER_SA_EMAIL}" --role='roles/pubsub.subscriber'
    gcloud projects add-iam-policy-binding ${PROJECT_ID} --member="serviceAccount:${PARTNER_SA_EMAIL}" --role='roles/storage.objectAdmin'
    gcloud projects add-iam-policy-binding ${PROJECT_ID} --member="serviceAccount:${PARTNER_SA_EMAIL}" --role='roles/editor'
    gcloud projects add-iam-policy-binding ${PROJECT_ID} --member="serviceAccount:${PARTNER_SA_EMAIL}" --role='roles/pubsub.editor'


    # Optionally create service account credentials
    gcloud iam service-accounts keys create --iam-account "${PARTNER_SA_EMAIL}" .gcp-service-account.json
    ```

4. Grant permissions to a GCP-managed Service Account
    1. Creates a custom role - `bigquerydatatransfer.connector` with permission `clientauthconfig.clients.getWithSecret`
    2. Grants roles `bigquerydatatransfer.connector`, `pubsub.admin`

    ```
    PROJECT_ID=$(gcloud config get-value core/project)
    GCP_SA_EMAIL="connectors@bigquery-data-connectors.iam.gserviceaccount.com"

    # Creating a custom role
    gcloud iam roles create bigquerydatatransfer.connector --project ${PROJECT_ID} --title "BigQuery Data Transfer Service Connector" --description "Custom role for GCP-managed Service Account for BQ DTS" --permissions "clientauthconfig.clients.getWithSecret" --stage ALPHA

    # Granting Service Account required roles
    gcloud projects add-iam-policy-binding ${PROJECT_ID} --member="serviceAccount:${GCP_SA_EMAIL}" --role="projects/${PROJECT_ID}/roles/bigquerydatatransfer.connector"
    gcloud projects add-iam-policy-binding ${PROJECT_ID} --member="serviceAccount:${GCP_SA_EMAIL}" --role='roles/pubsub.admin'
    ```

5. Join BigQuery Data Transfer Service Partner-level whitelists.  Reach out to your Google Cloud Platform contact to get whitelisted for these APIs.

## Understanding the BigQuery Data Transfer Service SDK example

* example/calendar_connector.py - Implementation of a BQ DTS connector
    * Handles Pub/Sub Message ack-deadline extensions
    * Flushes log messages to BQ DTS every --update-interval seconds
    * Stages data to Google Cloud Storage before invoking BQ loads

* example/data_source_definition.yaml - [YAML representation of a DataSourceDefinition](https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#datasourcedefinition)
    * Required for one-time setup of a DataSourceDefinition
    * Used in conjunction with bq_dts/bin/data_source_definition.py

* example/transfer_run.yaml - [YAML representation of a TransferRun](https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#transferrun)
    * \[DEV ONLY\] Mimics what would be received via a Pub/Sub subscription

* example/imported_data_info.yaml - [Partial YAML representation of StartBigQueryJobsRequest.ImportedDataInfo](https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#startbigqueryjobsrequest)
    * Mapping of table name to table schemas
    * `destination_table_id_template` is Python-formatted string used by `base_connector.templatize_table_name` and `base_connector.table_stager`

### Working with Google Cloud Platform auth
Prior to using the below examples, ensure you have set the following environment variables

* GOOGLE_APPLICATION_CREDENTIALS={path-to/.gcp-service-account.json}
* GOOGLE_CLOUD_PROJECT={project-id}


### Working with Data Source Definitions

    # Create
    python bq_dts/bin/data_source_definition.py --project-id {project-id} --location-id us --body-yaml example/data_source_definition.yaml create

    # List
    python bq_dts/bin/data_source_definition.py --project-id {project-id} --location-id us list

    # Get
    python bq_dts/bin/data_source_definition.py --dsd-name {dsd-name} get

    # Patch
    python bq_dts/bin/data_source_definition.py --dsd-name {dsd-name} --update-mask supportedLocationIds,dataSource.updateDeadlineSeconds --body-yaml example/data_source_definition.yaml patch


### Enrolling in a data source for testing purposes

    # Enroll in the Data Source
    python bq_dts/bin/data_source_definition.py --project-id {project-id} --location-id us --dsd-name {dsd-name} enroll


### Running the example app, serving BQ DTS requests

    # Development
    python example/calendar_connector.py --gcs-tmpdir gs://{gcs_bucket}/{blob_prefix}/ --transfer-run-yaml example/transfer_run.yaml example/imported_data_info.yaml

    # Production
    python example/calendar_connector.py --gcs-tmpdir gs://{gcs_bucket}/{blob_prefix}/ --ps-subname bigquerydatatransfer.{datasource-id}.{location-id}.run --use-bq-dts example/imported_data_info.yaml


## Running locally on Mac OS X (no K8s)
### Install dependencies


    # Requires Python 3.6
    brew install python3
    virtualenv env --python /usr/local/bin/python3.6
    source env/bin/activate
    pip install -r requirements.txt

## Building remotely on GKE-managed K8s cluster
### Create a GKE-managed K8s Cluster

    # Starts up a 3x f1-micro backed cluster in us-central1
    NAME=micro-cluster-ha
    ZONE=us-central1-b
    ADDITIONAL_ZONES=us-central1-c,us-central1-f
    MACHINE_TYPE=f1-micro

    gcloud config set compute/zone ${ZONE}
    gcloud beta container clusters create ${NAME} --zone ${ZONE} --additional-zones ${ADDITIONAL_ZONES} --machine-type ${MACHINE_TYPE} --num-nodes=1 --enable-autoupgrade --enable-autorepair

### Updating kubectl / Docker contexts

    # Switch to default docker config
    kubectl config use-context $(kubectl config get-contexts -o name | grep gke_)
    unset DOCKER_TLS_VERIFY
    unset DOCKER_HOST
    unset DOCKER_CERT_PATH
    unset DOCKER_API_VERSION

### Disable Docker "Securely store Docker logins in MacOS keychain"

    https://docs.docker.com/docker-for-mac/#general


### Building Docker Image for GKE

    IMAGE_NAME=bq-dts-partner-connector
    IMAGE_VERSION=$(date +"%Y%m%d-%H%M")

    PROJECT_ID="$(gcloud config get-value project)"
    REGISTRY_PREFIX="gcr.io/${PROJECT_ID/://}"
    REGISTRY_IMAGE=${REGISTRY_PREFIX}/${IMAGE_NAME}

    # Build locally since Container Builder doesn't respect .Dockerfile
    docker build -t ${REGISTRY_IMAGE}:${IMAGE_VERSION} .
    docker tag ${REGISTRY_IMAGE}:${IMAGE_VERSION} ${REGISTRY_IMAGE}:latest

    gcloud docker -- push ${REGISTRY_IMAGE}:${IMAGE_VERSION}
    gcloud docker -- push ${REGISTRY_IMAGE}:latest


## Building locally on a Minikube K8s cluster
### Install Minikube
1. Install [Docker for Mac](https://www.docker.com/docker-mac)

2. Install [VirtualBox](https://www.virtualbox.org/wiki/Downloads)

3. Install `kubectl`:


    gcloud components install kubectl


4. Install [Minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/)


### Updating kubectl / Docker contexts


    kubectl config use-context minikube
    eval $(minikube docker-env)


### Building Docker Image

    IMAGE_NAME=bq-dts-partner-connector
    docker build -t ${IMAGE_NAME}:latest .


## Deployment notes
### Updating K8's Deployment Dependencies

    # NOTE - When deploying to your K8s cluster, CHANGE THESE VARIABLES
    GCP_SERVICE_ACCOUNT_CREDS=.gcp-service-account.json
    BQ_DTS_PARTNER_CONNECTOR_CONFIG=example/imported_data_info.yaml
    K8_DEPLOYMENT=kube-deploy.minikube.yaml

    kubectl delete secret bq-dts-partner-connector-service-account || true
    kubectl create secret generic bq-dts-partner-connector-service-account --from-file ${GCP_SERVICE_ACCOUNT_CREDS}
    kubectl delete configmap bq-dts-partner-connector-config || true
    kubectl create configmap bq-dts-partner-connector-config --from-file config.yaml=${BQ_DTS_PARTNER_CONNECTOR_CONFIG}

    kubectl apply -f ${K8_DEPLOYMENT}

### Gracefully updating an existing K8's deployment

    REPLICAS=$(kubectl get deployment/${APPNAME} -o jsonpath='{.spec.replicas}')
    kubectl scale --replicas=0 deployment ${APPNAME}
    kubectl scale --replicas=${REPLICAS} deployment ${APPNAME}

### Describing deployments and/or pods


    kubectl describe deployments bq-dts-partner-connector
    kubectl describe pods $(kubectl get pods -l app=bq-dts-partner-connector -o name)


### Observing container logs


    kubectl logs -f $(kubectl get pods -l app=bq-dts-partner-connector -o name)


### Reference Reading
* https://cloud.google.com/container-engine/docs/tutorials/authenticating-to-cloud-pubsub
* https://ryaneschinger.com/blog/using-google-container-registry-gcr-with-minikube/
