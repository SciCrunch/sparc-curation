# Graph load and deployment
To load the graph run the following from the folder of this readme.
``` bash
ontload graph NIF-Ontology NIF -z /tmp/test1 -l /tmp/test1 -b dev -p -t ./graphload.yaml
```
To create services.yaml run the following from the folder of this readme.
``` bash
scigraph-deploy config --local --services-config ./services.yaml --services-user ec2-user --zip-location ./ localhost scigraph.scicrunch.io
```
See the [RPM Builds](https://github.com/tgbugs/pyontutils/blob/master/scigraph/README.md#rpm-builds) secion of the
[the pyontutils scigraph readme](https://github.com/tgbugs/pyontutils/blob/master/scigraph/README.md) for the rest
of the instructions for deployment.
