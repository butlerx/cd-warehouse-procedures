# CD-Procedures

A repository for the SQL procedures behind the CoderDojo Foundation data warehouse.

## Deploy
This image is built by Docker Hub

## Run in production
It takes the latest image and run via a cron
Elsewhat, you can create a one-type job by running
`kubectl --namespace xxx delete jobs/warehouse-migration && kubectl --namespace xxx create -f k8s/job.yml`
