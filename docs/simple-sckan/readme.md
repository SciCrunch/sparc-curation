# About Simple SCKAN
What do we mean by **Simple SCKAN**? Simple SCKAN refers to an extension of the [SCKAN](https://zenodo.org/record/6369432#.YxkD5OzML0q) that allows writing queries about the [NPO's](https://link.springer.com/article/10.1007/s12021-022-09566-7) core connectivity knowledge in a simplified manner.  The key objective of Simple SCKAN is to support simplicity while writing and testing the queries against NPO's complex, axiom-level connectivity statements. Simple SCKAN adds that query-friendly abstraction layer on top of the SCKAN.
- [Accessing Simple SCKAN](#accessing-simple-sckan)
- [Running Simple SCKAN Queries in Stardog](#running-simple-sckan-queries-in-stardog)
- [SCKAN Vs. Simple SCKAN](#sckan-vs-simple-sckan)
- [Query Example: SCKAN Vs. Simple SCKAN](#query-example-sckan-vs-simple-sckan)

# Accessing Simple SCKAN
* You can download **Simple SCKAN** ontology from the following link and load it into any standard Graph Databese such as Stardog, Graph DB, or Neo4J.
	* [Downlod Simple SCKAN in Turtle format from here](https://github.com/SciCrunch/NIF-Ontology/releases/download/sckan-2022-08-pre-3/simple-sckan-merged-with-npo.ttl).
* We recommend using [Stardog Studio](https://www.stardog.com/studio/) as the SPARQL query interface for Simple SCKAN. A Stardog endpoint is set up by the [FDI Lab](https://www.fdilab.org/) to provide access to the SCKAN connectivity knowledge via Stardog Studio. 
    * We have a set of predefined 'canned' query patterns avialable via Stardog Studio that we think would be useful for the users like SPARC's NLP curators, anatomical experts, and other interested users.
* What to expect in the future:
    * Have the Simple SCKAN graph available via [SciGraph](https://github.com/SciCrunch/SciGraph) so that the SciGraph users can utilize the benifits of Simple SCKAN while writing queries relevant to their tools and apps.
    * We will consider setting up a publicly available SPARQL endpoint for the Simple SCKAN.


# Running Simple SCKAN Queries in Stardog

*	Contact [SPARC K-Core](mailto:kcore@sparc.science) for the username and password to access the FDI Lab's Stardog endpoint
*	Load **Stardog Studio** from your web browser (Chrome works better)
    * Link: https://stardog.studio/#/
*	Do the following to connect to our FDI Lab’s Stardog endpoint:
	* Press the **Red Power Button** at the bottom-left corner of the page
	* On **Connect to Stardog** window, select **New Connection** and enter the following: 
		* Username: Your username
		*	Password: Your password
		*	Endpoint: https://stardog.scicrunch.io:5821
	*	Click **Add to My Connections** checkbox 
		*	Enter a **Name** for your connection (e.g., SPARC-USER)
*	Once connected, do the following:
	*	Click on the **Workspace** icon at the top-left corner
		*	Simply click **All Databases** dropdown box and select **NPO**
	* Select one of the **STORED** queries under **NPO**
		*	Run selected the query by pressing the **Blue Run Button** on the right panel
		* The comments in the queries should provide information on how to customize the queries

# SCKAN Vs. Simple SCKAN
The underlying ontologies in SCKAN are developed using standard OWL-DL formalism. The ontology that formalizes the SCKAN's core connectivity knowledge, the [Neuron Phenotype Ontology](https://link.springer.com/article/10.1007/s12021-022-09566-7) (NPO), is also encoded in OWL-DL. When it comes to the NPO, the main reasons for choosing the OWL-DL formalism are the following: (a) explicitly specifying the neuronal phenotypes with precise logical axioms and quantifiers, and (b) utilizing standard ontology reasoners for automated classification of neuron types, and detecting inconsistencies within the asserted axioms of the neuron populations. 
	
  * While OWL-DL supports automated reasoning and provides a rigorous formalism to specify the logical axioms of an ontology, it lacks support for quiring and retrieving those axiom-level specifications. We simply don't have enough tool support for complex axiom-level queries that one could use against the OWL ontologies.
  * While one can use SPARQL or CYPHER to retreive axiom-level knowledge from the OWL ontologies, it usually requires an extensive knowledge about the domain of the ontologies as well as the technical jargon of the OWL/RDF formalisms. 
  * The standard graph-based query laguages like SPARQL and CYPHER are meant for writing data or individual-level queries against a knowledgebase. They are not very suitable for writing the class-level queries with complex ontological axioms. Writing and testing such class-level queries against an OWL ontology can become quite difficult and tedious.
  * Since the neuron types in SCKAN are defined based on a set of OWL logical axioms, SCKAN suffers the exact same pitfalls above when it comes to retreiving the neuronal connectivity knowledge from its ontologies.
 
The **Simple SCKAN** refers to an extension of the SCKAN that incorporates a set of simplified subject-predicate-object relations about the core connectivity knowledge available in NPO. The key objective of the Simple SCKAN is to **support simplicity** while writing and testing the queries against NPO's complex, axiom-level connectivity statements. Following is the list of simplified relational predicates available in Simple SCKAN to express the connectivity between a neuron type and its locational phenotypes:

  * _**hasSomaLocation**_: a relation between a Neuron Type and its Soma Location 
  * _**hasAxonLocation**_: a relation between a Neuron Type and its Axon Location  
  * _**hasDendriteLocation**_: a relation between a Neuron Type and its Dendrite Location 
  * _**hasAxonTerminalLocation**_: a relation between a Neuron type and its Axon Terminal Location (i.e., the location of the axon presynaptic element)
  * _**hasAxonSensoryLocation**_: a relation between a Neuron type and its Sensory Axon Terminal Location (i.e., the location of the axon sesory subcellular element)

The relational properties above in **Simple SCKAN** serve as the 'shortcuts' for the NPO's actaul ontological axioms about the locational phenotypes of the neuron populations. Essentially, Simple SCKAN encapsulates those complex OWL axioms relevant to the NPO's locational phenotypes into simpler RDF graph patterns. The sole purpose of this encasulation is to allow querying and retrieiving SCKAN's connectivity knowledge in a simplified, managable manner. The next section provides an example as to how Simple SCKAN queries can be simpler to write than that of SCKAN using SPARQL.

## Query Example: SCKAN Vs. Simple SCKAN

* Here is an example query to find all the orgins (soma locations) and the termination regions of the neuronal connections specified in the [ApINATOMY](https://scicrunch.org/sawg/about/ApiNATOMY) models. In other words, this query asks where the origins of the ApINATOMY connections are and where do they terminate.

```SPARQL
# This is the SCKAN Query Example in SPARQL.

PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX partOf: <http://purl.obolibrary.org/obo/BFO_0000050>
PREFIX ilxtr: <http://uri.interlex.org/tgbugs/uris/readable/>

SELECT 
DISTINCT ?Neuron_Label ?Soma_Location_ID ?Soma_Location 
  				  ?Terminal_Location_ID ?Terminal_Location
WHERE
{
    VALUES (?npo_soma) {(ilxtr:hasSomaLocatedIn)}
    VALUES (?npo_terminal) 
      { 
        (ilxtr:hasAxonPresynapticElementIn)
        (ilxtr:hasAxonSensorySubcellularElementIn)
      }
    ?Neuron_ID owl:equivalentClass [
                                    rdf:type owl:Class ;
                                    owl:intersectionOf ?bn0
                                   ] .
    ?bn0 rdf:rest*/rdf:first ilxtr:NeuronApinatSimple .
    
    ?bn0 rdf:rest*/rdf:first [
                               rdf:type owl:Restriction ;
                               owl:onProperty ?npo_soma ;
                               owl:someValuesFrom [
                                                    a owl:Restriction ;
                                                    owl:onProperty partOf: ;
                                                    owl:someValuesFrom ?Soma_Location_ID
                                                  ] 
                              ] .
    ?bn0 rdf:rest*/rdf:first [
                               rdf:type owl:Restriction ;
                               owl:onProperty ?npo_terminal ;
                               owl:someValuesFrom [
                                                    a owl:Restriction ;
                                                    owl:onProperty partOf: ;
                                                    owl:someValuesFrom ?Terminal_Location_ID
                                                  ] 
                              ] .
    ?Neuron_ID rdfs:label ?Neuron_Label .
    ?Soma_Location_ID rdfs:label ?Soma_Location .
    ?Terminal_Location_ID rdfs:label ?Terminal_Location
  }
ORDER BY ?Neuron_ID ?Soma_Location ?Terminal_Location
LIMIT 999
```


```SPARQL
# This is the Simple SCKAN Query Example in SPARQL.

PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX partOf: <http://purl.obolibrary.org/obo/BFO_0000050>
PREFIX ilxtr: <http://uri.interlex.org/tgbugs/uris/readable/>

SELECT DISTINCT ?Neuron_Label ?Soma_Location_ID ?Soma_Location 
                               		 ?Terminal_Location_ID ?Terminal_Location
{
    ?Neuron_ID rdfs:subClassOf ilxtr:NeuronApinatSimple;  
                ilxtr:hasSomaLocation ?Soma_Location_ID;  
                (ilxtr:hasAxonTerminalLocation | ilxtr:hasAxonSensoryLocation) ?Terminal_Location_ID;
                rdfs:label ?Neuron_Label.
 
   ?Terminal_Location_ID rdfs:label ?Terminal_Location.
   ?Soma_Location_ID rdfs:label ?Soma_Location.
}
ORDER BY ?Neuron_ID ?Soma_Location ?Terminal_Location
LIMIT 999

```
## Query Result (first few results)
| Neuron_Label          | Soma_Location_ID | Soma_Location                          | Terminal_Location_ID | Terminal_Location                      |
|-----------------------|------------------|----------------------------------------|----------------------|----------------------------------------|
| neuron type aacar 1   | UBERON:0009050   | nucleus of solitary tract              | UBERON:0002022       | insula                                 |
| neuron type aacar 10a | ILX:0793555      | atrial intrinsic cardiac ganglion      | ILX:0793555          | atrial intrinsic cardiac ganglion      |
| neuron type aacar 10v | ILX:0793556      | ventricular intrinsic cardiac ganglion | ILX:0793556          | ventricular intrinsic cardiac ganglion |
| neuron type aacar 11  | UBERON:0005363   | inferior vagus X ganglion              | UBERON:0000947       | aorta                                  |
| neuron type aacar 11  | UBERON:0005363   | inferior vagus X ganglion              | UBERON:0002348       | epicardium                             |
| neuron type aacar 11  | UBERON:0005363   | inferior vagus X ganglion              | UBERON:0002084       | heart left ventricle                   |
| neuron type aacar 11  | UBERON:0005363   | inferior vagus X ganglion              | UBERON:0002079       | left cardiac atrium                    |
| neuron type aacar 11  | UBERON:0005363   | inferior vagus X ganglion              | UBERON:0009050       | nucleus of solitary tract              |
| neuron type aacar 11  | UBERON:0005363   | inferior vagus X ganglion              | UBERON:0002012       | pulmonary artery                       |
| neuron type aacar 11  | UBERON:0005363   | inferior vagus X ganglion              | UBERON:0002078       | right cardiac atrium                   |




# More **Simple SCKAN** Example Queries
	* Link will be posted here.
