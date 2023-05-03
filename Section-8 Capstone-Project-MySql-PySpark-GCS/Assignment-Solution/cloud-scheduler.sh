gcloud services enable cloudscheduler.googleapis.com

gcloud projects add-iam-policy-binding {project-id} \
  --member serviceAccount:{project-number}-compute@developer.gserviceaccount.com \
  --role roles/workflows.invoker


gcloud scheduler jobs create http spark-workflow-scheduler \
--schedule="0 9 * * 1" \
--uri="https://dataproc.googleapis.com/v1/projects/{project-id}/regions/us-central1/workflowTemplates/{wf-template-name}:instantiate?alt=json" \
--location="us-central1 " \
--message-body="{\"argument\": \"trigger_workflow\"}" \
--time-zone="America/New_York" \
--oauth-service-account-email="{project-number}-compute@developer.gserviceaccount.com"