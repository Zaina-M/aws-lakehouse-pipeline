.PHONY: install test lint package upload tf-init tf-plan tf-apply clean

ENV    ?= dev
TFDIR := envs/$(ENV)

install:
	pip install -r requirements-dev.txt

test:
	pytest tests/ -v --tb=short

lint:
	flake8 glue_jobs/ --max-line-length=120 --extend-ignore=E203,W503

package:
	mkdir -p dist
	cd glue_jobs && zip -r ../dist/etl_libs.zip utils/ jobs/

upload: package
	aws s3 cp dist/etl_libs.zip s3://$(SCRIPTS_BUCKET)/glue_jobs/etl_libs.zip
	aws s3 cp glue_jobs/main.py s3://$(SCRIPTS_BUCKET)/glue_jobs/main.py

tf-init:
	cd $(TFDIR) && terraform init

tf-plan:
	cd $(TFDIR) && terraform plan -var-file=../../envs/terraform.tfvars

tf-apply:
	cd $(TFDIR) && terraform apply -var-file=../../envs/terraform.tfvars

clean:
	rm -rf dist/ .pytest_cache/ __pycache__/ glue_jobs/**/__pycache__/
