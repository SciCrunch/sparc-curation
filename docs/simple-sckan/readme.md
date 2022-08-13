# About SIMPLE-SCKAN
What do we mean by **SIMPLE-SCKAN**? SIMPLE-SCKAN refers to an extension of the SCKAN that allows writing queries about the NPO's core connectivity knowledge in a simplified manner.  The key objective of SIMPLE-SCKAN is to **_support simplicity_** while writing and testing the queries against NPO's axiom-level, complex ontological connectivity statements. SIMPLE-SCKAN adds that 'query simplification layer' on top of the SCKAN.
- [Accessing SIMPLE-SCKAN](#accessing-simple-sckan)
- [Running SIMPLE-SCKAN Queries in Stardog](#running-simple-sckan-queries-in-stardog)
- [SCKAN Vs. SIMPLE-SCKAN](#sckan-vs-simple-sckan)
- [Query Example: SCKAN Vs. SIMPLE-SCKAN](#query-example-sckan-vs-simple-sckan)

# Accessing SIMPLE-SCKAN
* You can download **SIMPLE-SCKAN** ontology from the following link and load it into any standard Graph Databese such as Stardog, Graph DB, or Neo4J.
	* [**Downlod SIMPLE-SCKAN in Turtle format from here**](https://github.com/SciCrunch/NIF-Ontology/releases/download/sckan-2022-08-pre-2/sckan-npo-simple.ttl).
* We recommend using **Stardog Studio** as the SPARQL query interface to query over the NPO/SCKAN connectivity knowledge
    * A Stardog endpoint is set up by the FDI lab to support the **Stardog Stuio** interface for the users like SCKAN's NLP curators to check whether a connection exists between regions based on a set of neuron populations in NPO
    * We have a set of predefined "canned" query patterns avialable via Stardog that we think would be useful for the NLP curators and other interested users.
* What to expect in the future:
    * Have the Simple SCKAN graph available via SciGraph so that the SciGraph users can develop different tools and apps based on simpler queries using SPARQL or Cypher.
    * We also want to have the Stardog interface available for broader audience who would prefer a simpler way to query SCKAN connectivity knowledge.


# Running SIMPLE-SCKAN Queries in Stardog

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
	* Select one of the **STORED** queries under NPO
		*	Run the query by pressing the **Blue Run Button** on the right panel
		* The comments in the queries should provide information on how to customize the queries

# SCKAN Vs. SIMPLE-SCKAN
* The underlying ontologies for SCKAN are developed using standard OWL-DL formalism. The two main reasons for choosing the OWL-based formalism are the following: (a) defining the neuron types with precise logical axioms and quantifiers, and (b) utilizing standard inference engines or reasoners for automated classification of neuron types, detecting inconsistencies in the asserted axioms, and inferring implicit logical axioms from the explicit ones. 
	* While OWL provides a useful formalism to specify the logical axioms of an ontology in a rigorous manner, it lacks the support for quiring and retreiving those axiom-level knowledge. There is simply not enough tool-support available for complex axiom-level queries that one could use against the OWL ontologies. 
	* The graph-based standard query laguages such as SPARQL and CYPHER are mostly meant for writing data or individual-level queries against a knowledgebase (comibination of ontological classes and their instantiations with actual instance data). 
		* While one can use SPARQL or Cypher to retreive axiom-level knowledge from an OWL ontology, it usually requires an extensive knowledge both about the domain and the structure of the ontology, as well as the technical jargon of the OWL/RDF formalisms. OWL class-level queries with complex logical axioms can be difficult and tedious to write and test using SPARQL or Cypher. 
	* Since the neuron types in NPO/SCKAN ontologies are defined based on a set of OWL axioms, SCKAN suffers the exact same pitfalls above when it comes to retreiving the neuronal connectivity knowledge from its ontologies. 
* **SIMPLE-SCKAN** refers to an extension of the SCKAN that incorporates a set of simplified _**subject-predicate-object**_ relations about the core connectivity knowledge available in NPO. The key objective of the **SIMPLE-SCKAN** is to **_support simplicity_** while writing and testing the queries against NPO's axiom-level connectivity knowledge. Following is the list of simplified relational predicates available in SIMPLE-SCKAN to express the connectivity between a neuron type and its locational phenotypes:
	* _**hasSomaLocation**_: a relation between a Neuron Type and its Soma Location 
	* _**hasAxonLocation**_: a relation between a Neuron Type and its Axon Location  
	* _**hasDendriteLocation**_: a relation between a Neuron Type and its Dendrite Location 
	* _**hasAxonTerminalLocation**_: a relation between a Neuron type and its Axon Terminal Location (i.e., the location of the axon presynaptic element)
	* _**hasAxonSensoryLocation**_: a relation between a Neuron type and its Sensory Axon Terminal Location (i.e., the location of the axon sesory subcellular element)
* The relational properties above in **SIMPLE-SCKAN** serve as the 'shortcuts' for the actaul ontological relations specified by the NPO to express its axioms about the locational phenotypes of the neuron populations. Essentially, SIMPLE-SCKAN encapsulates the complex OWL axioms relevant to the NPO's locational phenotypes into simpler RDF graph patterns. The sole purpose of this encasulation is to allow querying and retrieiving SCKAN's connectivity knowledge in a simplified, managable manner. 
* The next section provides an example as to how SIMPLE-SCKAN queries can be simpler to write than that of SCKAN using SPARQL.

## Query Example: SCKAN Vs. SIMPLE-SCKAN

* Here is an example query to find all the orgins (soma locations) and the termination regions of the neuronal connections specified in the ApINATOMY models. In other words, this query asks where the origin of the ApINATOMY connections are and where do they terminate.

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
# This is the SIMPLE-SCKAN Query Example in SPARQL.

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




# More **SIMPLE-SCKAN** Example Queries
	* Link will be posted here.