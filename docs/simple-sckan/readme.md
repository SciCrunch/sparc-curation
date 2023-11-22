# About Simple SCKAN

What do we mean by **Simple SCKAN**? Simple SCKAN refers to an extension of the [SCKAN](https://zenodo.org/record/6369432#.YxkD5OzML0q) that allows writing queries about [NPO&#39;s](https://link.springer.com/article/10.1007/s12021-022-09566-7) core connectivity knowledge in a simplified manner.  The key objective of Simple SCKAN is to support simplicity while writing and testing the queries against NPO's complex, axiom-level connectivity statements. Simple SCKAN adds that query-friendly abstraction layer on top of the SCKAN.

- [Accessing Simple SCKAN](#accessing-simple-sckan)
- [Running Simple SCKAN Queries in Stardog](#running-simple-sckan-queries-in-stardog)
- [SCKAN Vs. Simple SCKAN](#sckan-vs-simple-sckan)
- [Query Example: SCKAN Vs. Simple SCKAN](#query-example-sckan-vs-simple-sckan)
- [Transforming SCKAN into Simple SCKAN](#transforming-sckan-into-simple-sckan)

# Accessing Simple SCKAN

* You can use [Stardog Studio](https://www.stardog.com/studio/) as the SPARQL query interface for Simple SCKAN. A Stardog endpoint is set up by the [FDI Lab](https://www.fdilab.org/) to provide access to SCKAN via Stardog Studio.
  * We have a set of predefined 'canned' query patterns avialable via Stardog Studio that we think would be useful for the users like SPARC's NLP curators, anatomical experts, and other interested users.
  * Refer to the next section on [Running Simple SCKAN Queries in Stardog](#running-simple-sckan-queries-in-stardog).
* We also have a publicly available Blazegraph SPARQL endpoint for both SCKAN and Simple SCKAN
  * Blazegraph Endpoint: [https://blazegraph.scicrunch.io/blazegraph/sparql](https://blazegraph.scicrunch.io/blazegraph/sparql)
  * No username or password needed to access this endpoint.
  * Check out the Notebook on [SCKAN Query Examples](https://github.com/smtifahim/sckan-query-examples/tree/main) using Simple SCKAN predicates.
* If you simply want to search the key contents of the SCKAN without writing SPARQL queries, please use  [SCKAN Explorer](https://services.scicrunch.io/sckan/explorer/).
  * SCKAN Explorer is an intuitive, web-based search interface to explore the SPARC connectivity knowledge for non-technical domain experts.
  * Behind the scene, SCKAN Explorer uses simplified SPARQL queries supported by Simple SCKAN.

# Running Simple SCKAN Queries in Stardog

If you *don't* have the username and password to access our Stardog server, please contact [SPARC K-Core](mailto:kcore@sparc.science). If you already have the username and password, follow the instructions below.

1. Go to [https://cloud.stardog.com]() from your web browser.
   * **Sign Up** or **Log In** to Stardog to continue to [Stardog Cloud]([https://cloud.stardog.com]()) using your email address and password
   * **Note:** Please use Chrome or Firefox as your browser. Stardog Studio does not function properly when using Safari.
2. Set up a new connection on the Stardog Cloud main dashboard.
   * Click on the '**+ New Connection'** button and then on **Connect to Stardog** window, select **New Connection**
   * Enter the following information:
     * Enter the **Username** and **Password** given to you by SPARC K-Core
     * **Endpoint:** [https://sd-c1e74c63.stardog.cloud:5820](https://sd-c1e74c63.stardog.cloud:5820)
     * Enter a name for your connection (e.g., SPARC-User)
   * Click on the '**Connect**' button to get connected with our server
3. Launch Stardog Studio on your browser.
   * Click on the connection containg the endpoint above and navigate to the server's dashboard
   * Click on the **STARDOG STUDIO** section and launch the Stardog Studio interface
4. Run **Simple SCKAN** queries using Stardog Studio.
   * Click on the **Workspace** icon from the left (the icon that looks like a tiny piece of paper folded in its corner)
   * Click **All Databases** dropdown box and then select **NPO**
   * Select one of the **STORED** queries under **NPO** and on the right panel, click **Select Database** and select **NPO** again
     * Run the selected the query by pressing the **Blue Run Button** on the right panel
     * The comments within the SPARQL queries should provide information on how to customize the queries

# SCKAN Vs. Simple SCKAN

The underlying ontologies in SCKAN are developed using standard OWL-DL formalism. The ontology that formalizes the SCKAN's core connectivity knowledge, the [Neuron Phenotype Ontology](https://link.springer.com/article/10.1007/s12021-022-09566-7) (NPO), is also encoded in OWL-DL. When it comes to the NPO, the main reasons for choosing the OWL-DL formalism are the following: (a) explicitly specifying the neuronal phenotypes with precise logical axioms and quantifiers, and (b) utilizing standard ontology reasoners for automated classification of neuron populations, and detecting inconsistencies within the asserted axioms of the neuron populations.

* While OWL-DL supports automated reasoning and provides a rigorous formalism to specify the logical axioms of an ontology, it lacks support for quiring and retrieving those axiom-level specifications. We simply don't have enough tool support for complex axiom-level queries that one could use against the OWL ontologies.
* While one can use SPARQL or CYPHER to retreive axiom-level knowledge from the OWL ontologies, it usually requires an extensive knowledge about the domain of the ontologies as well as the technical jargon of the OWL/RDF formalisms.
* The standard graph-based query laguages like SPARQL and CYPHER are meant for writing data or individual-level queries against a knowledgebase. They are not very suitable for writing the class-level queries with complex ontological axioms. Writing and testing such class-level queries against an OWL ontology can become quite difficult and tedious.
* Since the neuron populations in SCKAN are defined based on a set of OWL logical axioms, SCKAN suffers the exact same pitfalls above when it comes to retreiving the neuronal connectivity knowledge from its ontologies.

**Simple SCKAN** refers to an extension of the SCKAN that incorporates a set of simplified subject-predicate-object relations about the core connectivity knowledge available in NPO. The key objective of the Simple SCKAN is to **support simplicity** while writing and testing the queries against NPO's complex, axiom-level connectivity statements. Following is the list of simplified relational predicates available in Simple SCKAN:

#### Locational Phenotypes

| Phenotypic Relation               | Description                                                                                                                                           |
| --------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **hasSomaLocation**         | Expresses a relation between a neuron population and its origin i.e., location of the cell body                                                       |
| **hasAxonLocation**         | Expresses a relation between a neuron population and its axon location                                                                               |
| **hasDendriteLocation**     | Expresses a relation between a neuron population and its dendrite location                                                                           |
| **hasAxonTerminalLocation** | Expresses a relation between a neuron population and its axon terminal location<br />(i.e., the location of the axon presynaptic element)             |
| **hasAxonSensoryLocation**  | Expresses a relation between a neuron population and its sensory axon terminal location (i.e., the location of the axon sensory subcellular element) |

#### Other Phenotypes

| Phenotypic Relation                | Description                                                                                                                                                                                                                                                                                                                      |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **hasNeuronalPhenotype**     | Expresses a relation between a neuron population and its ANS phenotype<br />- **Pre-Ganglionic or Post-Ganglionic**, **Sympathetic** or **Parasympathetic**<br />- Combinations like  **Sympathetic  Pre-Ganglionic** or **Parasympathetic Post-Ganglionic**                                    |
| **hasFunctionalCircuitRole** | Expresses a relation between a neuron population and its immediate effect on postsynaptic cells<br />- **Excitatory** or **Inhibitory**                                                                                                                                                                           |
| **hasCircuitRole**           | Expresses a relation between a neuron population and its circuit role phenotype<br />- Possible phenotypes are: **Intrinsic**, **Motor**, **Sensory**, or **Projection**                                                                                                                               |
| **hasProjection**            | Expresses a relation between a neuron population and a brain region where the neuron population sends axons towards<br />- **Spinal cord descending projection**, **Spinal cord ascending projection**<br />- **Intestino fugal projection**, **Anterior projecting**, **Posterior projecting** |
| **isObservedInSpecies**      | Expresses a relationship between a neuron type and a taxon. Used when a neuron population has een observed in a specific species.<br />- **Species from NCBI Taxonomy**                                                                                                                                                   |
| **hasPhenotypicSex**         | Expresses a relationship between a neuron type and a biological sex. Used when a neuron population has been observed in a specific sex.<br />- **Male or **Female** from PATO**                                                                                                                                    |
| **hasForwardConnection**     | Expresses a relationship to specify the synaptic forward connection from a pre-ganglionic neuron population to a post-ganglionic neuron population.                                                                                                                                                                             |

The relational predicates above in **Simple SCKAN** serve as 'shortcuts' for the actual OWL axioms used in NPO to represent the phenotypic properties of its neuron populations. Essentially, Simple SCKAN encapsulates those complex OWL axioms into a set of simpler RDF graph patterns. The sole purpose of this encapsulation is to allow querying and retrieiving SCKAN's connectivity knowledge in a simplified, managable manner. The next section provides an example as to how the queries can be simpler to write using Simple SCKAN predicates as compared to that of using full SCKAN. The example also demonstrates how the Simple SCKAN version of the query is easier to comprehend and more managable to edit.

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
| --------------------- | ---------------- | -------------------------------------- | -------------------- | -------------------------------------- |
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

We have compiled a set of query examples supported by Simple SCKAN (linked below). These examples were written to demonstrate how to use Simple SCKAN predicats to retreive the key contenets of SCKAN using SPARQL.

* [Query examples in Jupyter Notebook](https://github.com/smtifahim/sckan-query-examples/)

# Transforming SCKAN into Simple SCKAN

The process of transforming SCKAN/NPO into Simple SCKAN is documented in the following link. The process is automated using a python script that takes care of generating all the necessary files needed for Simple SCKAN and loads them into the Stardog Server.

* [SCKAN to Simple SCKAN Transformation Process](https://github.com/smtifahim/Loading-Simple-SCKAN/tree/main/sckan-to-simple-sckan)
