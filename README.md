# Final Project Team ProMaG (Product & Market Growth)

Note: this is repository for local deployment. The source code for GCP Deployment is different from this

## How to run
1. Run docker desktop in the local system

2. Build docker image for Airflow 
```
docker build -t promag:latest -f dockerfile
``` 
3. Compose and run docker containers which contains PostgreSQL, pre-built Airflow, and Selenium Grid images.
```
docker-compose up
```

## How to use
Make a POST API call to `http://localhost:8080/api/v1/dags/tokped_etl/dagRuns` with the following format 
```
{
    "conf" : {"nama_produk" : "product_name","pages":num_of_pages}
}
```
Parameter details:

1. product_name (string) : name of the product you want to search in Tokopedia. The Tokopedia web page that will be scrpped is https://tokopedia.com/find/product_name
2. num_of_pages (integer) : number of pages you want to scrap. The Tokopedia website has format of maximum 80 products per page. The estimate time to scrape a single page is 20 minutes 

You can view and manage the ETL Workflow in the airflow interface at `http://localhost:8080/dags/tokped_etl/grid` <br>

The data will be stored in Google Cloud Storage (GCS) for Data Staging and BigQuery for analytics with Tableau Data Visualization.
