# k8analysis
Run data analysis jobs in Kubernetes.

Note: The code below is separate from the related functionality available in 
[pipeline5](https://github.com/hartwigmedical/pipeline5) and [platinum](https://github.com/hartwigmedical/platinum).
Results are not guaranteed to be identical or even similar.



### Available jobs

Job name | Description
---|---
align  | Run bwa mem alignment of paired reads FASTQ.
non-umi-dedup  | Deduplicate bam with sambamba markdup whilst ignoring UMI's.
umi-dedup  | Deduplicate bam with UMI-Collapse by taking UMI's into account.
flagstat  | Collect flagstat stats for bam with sambamba flagstat.
count-mapping-coords  | Count unique mapping positions present in bam.


### Running in Kubernetes
You need to have the right credentials and a cluster (only need this step once).
```shell script
gcloud container clusters get-credentials rerun-cluster --region europe-west4 --project hmf-crunch>
```


###Documentation
All commands provide more complete documentation when run with incomplete or incorrect arguments.
Some suggested commands to get you started:
```shell script
./k8analysis
./k8analysis run
./k8analysis run align
./k8analysis run non-umi-dedup
./k8analysis version
./k8analysis build
./k8analysis push
./k8analysis set-default
```

### Conditions FASTQ files
The FASTQ file names need to contain `_R1_` or `_R2_` to show whether they are read 1 or 2.
All read 1 FASTQ files need to have a corresponding read 2 FASTQ file and vice versa.
A read 1 FASTQ file corresponds to a read 2 FASTQ file when the only difference in their file names is the `_R1_` vs `_R2_`.



### Monitoring
```shell script
kubectl get jobs | grep <your-job-name>
kubectl get pods | grep <your-job-name>
kubectl logs <your-pod-name>
```