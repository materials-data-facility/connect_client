Supplemental Document for **DEPARTMENT OF COMMERCE**   
**RESEARCH PERFORMANCE PROGRESS REPORT (RPPR)** 

# Accomplishments

**24\. What were the major goals and objectives of this project?**  
In MDF Phase IV, we will build on prior MDF successes by exploring and researching the application of large language models (LLMs) on many common aspects of materials scientist daily work e.g.: 1\) helping to improve communication of results, 2\) increasing our service accessibility and simplicity, and 3\) automating complex metadata extraction. With the rapid pace of change seen in AI and LLMs in particular, we will prioritize maintaining an agile approach to our research, prototype development, and deployments that allows us to incorporate the latest unforeseen capabilities from the broader AI community, as new opportunities present themselves. We will also maintain the services deployed and data collected in all previous MDF Phases.

**25\. What was accomplished under these goals?**

## New MDF Spotlight Dataset Type

To better support the dissemination and discovery of high-impact datasets, the MDF team introduced and deployed the MDF Spotlight Dataset feature during this period (**Fig. 1**). The Spotlight dataset is a new dataset presentation type that allows curators and data contributors to create rich, custom HTML pages associated with their datasets. These Spotlight pages go beyond standard metadata presentation enabling datasets to include figures, tables, detailed descriptions of scientific context, methodology, and usage recommendations. This innovation addresses a critical gap in dataset discovery and accessibility by providing researchers with the longer-form narrative context and the visual information needed to quickly understand a dataset's significance and determine its relevance to their research.

The Spotlight feature has already been deployed for the OMol25 Electronic Structures dataset described below, serving as a model for future high-profile dataset releases. By combining compelling scientific presentation with direct data access, the Spotlight format enhances the value proposition of MDF as a platform for dataset dissemination and strengthens the connection between data and the scientific stories they tell.

| ![][image1] | ![][image2] |
| :---- | :---- |
| ![][image3] |  |
| **Fig. 1: Example showing the structure and depth of information available for MDF Spotlight datasets.** |  |

## Strategic Targeting of High-Profile Scientific Datasets

Building on the success of the OMol25 Electronic Structure Spotlight dataset and recognizing the significant opportunity for scientific value paired with community demand, the MDF team has identified and is actively pursuing partnerships with leading research groups to integrate more landmark datasets into the MDF ecosystem. These strategic targets represent some of the most scientifically valuable and largest-scale datasets in materials science and related fields with applications ranging from training new models to enable autonomous microscopy, understanding fatigue at the microstructural level, and the stability of metal-organic frameworks (MOFs).

* **100 TB Microscopy Dataset** from Jean-Charles Stinville at the University of Illinois: A comprehensive collection of advanced microscopy data representing a major experimental resource for materials characterization and discovery.  
* **50 TB Metal Fatigue Dataset** from Amelia Henricksen at Sandia National Laboratories: A critical dataset containing detailed metal fatigue and failure data of significant interest to the materials and aerospace communities.  
* **1 PB Simulated Molecular MOF Dataset** from Andrew Rosen at Princeton University: An exceptionally large collection of molecular dynamics simulations and calculations for metal-organic framework stability, relevant to applications in gas storage, separation, and catalysis.

These targeted collaborations reflect MDF's commitment to aggregating the most scientifically valuable open datasets and making them discoverable and accessible through a unified platform. By partnering with leading research institutions and national laboratories, we seek to position MDF as the primary hub for locating and accessing exceptional datasets for training state-of-the-art machine learning models that enable new discoveries in materials science, chemistry, and related fields.

## OMol25 Electronic Structures Dataset Integration and Community Support

The MDF team has completed a major collaborative effort with Meta to organize, curate, and make accessible the OMol25 Electronic Structures Dataset (**Fig. 2**), representing what we believe to be the largest and most diverse collections of open molecular electronic structure calculations (also relevant to problems in materials) ever assembled. The OMol25 Electronic Structures dataset comprises approximately **500 TB** of data hosted through MDF infrastructure at Argonne National Lab’s Eagle cluster, representing a curated subset of Meta's 10 PB total collection spanning over 100 million molecular structures calculated at the ωB97M-V/def2-TZVPD level of theory. This comprehensive resource encompasses simulations for biomolecules, electrolytes, metal complexes, and community datasets with applications across manufacturing, catalysis, batteries, pharmaceuticals, and materials design.

To highlight this landmark dataset and enable broad community access, the MDF team created an MDF Spotlight Dataset page at https://www.materialsdatafacility.org/spotlight/omol25, providing researchers with rich contextual information, figures, and detailed descriptions alongside direct data access. This initiative has already generated significant community engagement, with over 100 researchers and multiple large centers (at LLNL for drug discovery, the UK’s Ada Lovelace Center, several industrial drug discovery companies, and more) actively utilizing the dataset for machine learning and materials discovery applications. The breadth and depth of electronic structure data contained within OMol25 enables the creation of a new generation of machine learning algorithms previously limited by training data availability, positioning the materials science and chemistry communities to accelerate AI-driven discovery in these fields.

**Next Steps:** We are exploring creation of featurized summary datasets that will make the 500 TB dataset more usable by the community, and we have two active projects to create next generation Machine Learned Interatomic Potentials (MLIPs) with Sam Blau (LLNL) and Andrew Rosen (Princeton). Further, we are working with our partners at Argonne National Lab to make the entire 8-10 PB of data available to the community, a capability that is only possible via MDF’s unique infrastructure and expertise.

| ![][image4] |
| :---- |
| **Fig. 2: OMol25 Electronic Structures Dataset (ESD) overview figure. The OMol25 ESD provides full calculations for 4.4M molecules at extremely high fidelity for training MLIP models.** |

## Public Dataset Curation Effort

Access to high quality data is a primary bottleneck in the development of AI foundation models for scientific applications. Thus, the MDF team has established a comprehensive, community-driven repository cataloging open-access datasets available for materials science and chemistry machine learning model development (**Fig. 3**). This effort has identified, evaluated, and organized over **160 high-quality datasets** across five critical categories: 1\) computational datasets (e.g., density functional theory calculations and molecular dynamics simulations), 2\) experimental datasets (e.g., crystal structures and materials property measurements), 3\) LLM training datasets (e.g., instruction pairs, benchmarks, and domain-specific text corpora), 4\) computational fluid dynamics, and 5\) literature-mined collections (e.g., reaction databases and chemical patents).

The curated collection encompasses datasets ranging from billion-scale DFT calculation repositories to specialized resources like the Open Reaction Database with over 1 million synthetic reactions, representing a landscape of available training data encompassing billions of molecular structures, quantum mechanical calculations, and chemistry-focused text tokens. Each dataset entry includes critical metadata such as licensing information, data formats, access methods, size metrics, and recommended use cases, enabling researchers to quickly identify and appropriately utilize resources for their specific applications. The initiative has demonstrated substantial community interest, generating 255+ user stars on GitHub within just a few months of launch. Examples of collected datasets include:

1. **Computational Datasets** i.e., diverse quantum chemistry and materials simulation resources, such as:   
* DFT and molecular dynamics repositories (Materials Project, OQMD, JARVIS-DFT)  
* Molecular quantum chemistry databases (QM9, ANI-1x, SPICE)  
* Crystal structure collections (AFLOW, C2DB)  
* Materials for catalysis and gas storage (CoRE MOF, Open Catalyst Project)  
* Large-scale computational collections (OMat24, OMol25 with 100+ million calculations)  
2. **Experimental Datasets** that provide access to real-world materials characterization and property data such as  
* Crystal structure repositories (COD, ICSD, Cambridge CSD)  
* Bioactive molecules and pharmaceutical compounds (ChEMBL with 2.3+ million compounds)  
* Protein-ligand binding information (PDBbind, BindingDB)  
* Polymer properties and materials testing data  
* Spectroscopic and synthesis information  
3. **LLM Training Datasets** include curated text collections specifically designed for materials and chemistry domain adaptation:  
* Chemistry-focused instruction sets (ChemPile with 75+ billion tokens, SmolInstruct)  
* Question-answer pairs and domain knowledge (ChemNLP, ChemBench)  
* Materials science knowledge collections and scientific reasoning benchmarks (MegaScience, SciCode)  
* PubChem (119 million compounds)  
* Open Reaction Database (over 1 million reactions)  
* MatScholar (5+ million abstracts)

And much more.

While MDF has primarily focused on collecting and publishing data resources directly, this new community resource addresses the MDF mission to democratize access to all materials data and accelerate scientific discovery no matter where the data are located.

| ![][image5] | ![][image6] |
| :---- | :---- |
| **Fig. 3: Partial snapshots of the LLM and Experimental section of the dataset resource showing some of the cataloged resources for materials science and chemistry.** |  |

### Maintaining MDF Services

All production services from previous phases were maintained and operated at or above feature parity. Further, storage at NCSA (\~110 TB) was maintained and operated, 500 TB of new publication storage was obtained at Argonne through a merit allocation program.

**26\. What opportunities for training and professional development has the project provided?**  
Will Engler, Owen Price-Skelly, Ben Blaiszik, and others have participated in many workshops, meetings, and seminars to broaden understanding and connections with the materials community.

Several developers that are part of the MDF team are leveraging UChicago resources to attain certifications in Amazon Web Services and business school classes. 

In the early phase of this award, we have worked with 2 undergraduates from  the University of North Carolina in Asheville (Hayden Holbrook, Chase Jenkins). Hayden and Chase are working on the MDF website design and code implementation. Each student was mentored by the team to translate their skills from the classroom to a production level software platform. After this training, Chase obtained a full time job at Duck Creek Technologies and Hayden joined PI Blaiszik’s Team as a junior software developer.

**27\. How were the results disseminated to communities of interest?**  
The results were disseminated through standard academic routes including papers and presentations as listed in Section 29 and in previous reports. 

Further, we have continued to promote MDF datasets and work on Twitter, LinkedIn, Threads, and YouTube. We have seen some posts receive \>100,000 impressions, with the recent OMol25 release reaching 75k impressions and kick starting several academic and industrial collaborations. We believe the social arm of dissemination reaches a non-traditional audience, draws significant community interest, increases the community knowledge of materials data and informatics, and leads to new collaborations and more usage of MDF services and software.

**28\. What do you plan to do during the next reporting period to accomplish the goals and objectives?**  
In the next reporting period, we will continue to refine the MDF interfaces and expand our LLM integration efforts by developing automated metadata extraction capabilities. We will also write up our experiences from the most recent LLM Hackathon (to be included in the next report). We will also continue to collect and catalog links to the best datasets available in materials science and chemistry and seek to publish best-in-class datasets as Spotlight Datasets.

# Products

**29\. Publications, conference papers, and presentations**  
**Publications**

Zimmermann, Yoel, Adib Bazgir, Alexander Al-Feghali, Mehrad Ansari, Joshua Bocarsly, L. Catherine Brinson, Yuan Chiang, Defne Circi, Min-Hsueh Chiu, Nathan Daelman, Matthew L. Evans, Abhijeet S. Gangan, Janine George, Hassan Harb, Ghazal Khalighinejad, Sartaaj Takrim Khan, Sascha Klawohn, Magdalena Lederbauer, Soroush Mahjoubi, Bernadette Mohr, Seyed Mohamad Moosavi, Aakash Naik, Aleyna Beste Ozhan, Dieter Plessers, Aritra Roy, Fabian Schöppach, Philippe Schwaller, Carla Terboven, Katharina Ueltzen, Yue Wu, Shang Zhu, Jan Janssen, Calvin Li, Ian Foster, and Ben Blaiszik. 2025\. “**32 Examples of LLM Applications in Materials Science and Chemistry: Towards Automation, Assistants, Agents, and Accelerated Scientific Discovery.**” *Machine Learning: Science and Technology* 6 (3): 030701\. https://doi.org/10.1088/2632-2153/ae011a.

Baird, Sterling, Mehrad Ansari, Zartashia Afzal, Qianxiang Ai, Alexander Al-Feghali, Mathieu Alain, Matias Altamirano et al. **"Bayesian Optimization Hackathon for Chemistry and Materials."** (2025).

Lake, Jack R., Simon Rufer, Jim James, Nathan Pruyne, Aristana Scourtas, Marcus Schwarting, Aadit Ambadkar, Ian Foster, Ben Blaiszik, and Kripa K. Varanasi. **"Machine learning-guided discovery of gas evolving electrode bubble inactivation."** *Nanoscale* 17, no. 3 (2025): 1270-1281.

Jacobs, Ryan, Dane Morgan, Siamak Attarian, Jun Meng, Chen Shen, Zhenghao Wu, Clare Yijia Xie et al. "A practical guide to machine learning interatomic potentials–Status and future." *Current Opinion in Solid State and Materials Science* 35 (2025): 101214\. **Previously reported, but now at 85 citations**

**30\. Technologies or techniques**  
The software and services developed via this support are listed in Section 32 under “Other Products”.

**31\. Inventions, patent applications, and/or licenses**  
Nothing to report

**32\. Other products**  
Codes developed and maintained via this project are available through the MDF Github organization page at https://github.com/materials-data-facility. These projects are made openly available to the community for access and reuse. In this first report, these products represent software and services developed primarily in earlier phases of the project.

**Selected Code Repositories**  
Awesome Materials & Chemistry Dataset List (132 stars in the first month): [https://github.com/blaiszik/awesome-matchem-datasets](https://github.com/blaiszik/awesome-matchem-datasets)  
FAIR Foam Database: [https://github.com/materials-data-facility/foam\_db](https://github.com/materials-data-facility/foam_db)  
MDF Github Organization: [https://github.com/materials-data-facility](https://github.com/materials-data-facility)  
MDF Connect Client: [https://github.com/materials-data-facility/connect\_client](https://github.com/materials-data-facility/connect_client)  
MDF Connect Server: [https://github.com/materials-data-facility/connect\_server](https://github.com/materials-data-facility/connect_server)  
MaterialsIO: [https://github.com/materials-data-facility/MaterialsIO](https://github.com/materials-data-facility/MaterialsIO)  
MDF Forge: [https://github.com/materials-data-facility/forge](https://github.com/materials-data-facility/forge)  
ML-Ready Dataset Examples:  [https://github.com/MLMI2-CSSI/foundry](https://github.com/MLMI2-CSSI/foundry)   
   
**Globus Automate**  
Documentation: [https://globus-automate-client.readthedocs.io/en/latest/](https://globus-automate-client.readthedocs.io/en/latest/)  
Examples: [https://github.com/globus/automation-examples](https://github.com/globus/automation-examples)  
Automate Client: [https://github.com/globus/globus-automate-client](https://github.com/globus/globus-automate-client)

# Participants & Other Collaborating Organizations

**33\. What individuals have worked on this project?**  
See attached PDF

**34\. Has there been a change in the active other support of the PD/PI(s) or senior/key personnel since the last reporting period?**  
See attached C\&P for Blaiszik and Foster for precise details.

**35\. What other organizations have been involved as partners?**  
All funded partners for this project are located at the University of Chicago. Key collaborators are located at the University of Chicago, NIST, Argonne National Lab, Northwestern, the Center for Hierarchical Materials and Design, MIT, the University of Illinois at Urbana-Champaign, the University of Wisconsin-Madison, Globus, and more. Key collaborating industrial partners include 3M 

Organization Name: University of Chicago  
Organization Location: Illinois, USA  
Partner’s Contribution to the project: in kind (software development), collaboration   
More detail: domestic

Organization Name: NIST  
Organization Location: Maryland, USA  
Partner’s Contribution to the project: collaboration   
More detail on partner and contribution: domestic

Organization Name: Argonne National Lab  
Organization Location: Illinois, USA  
Partner’s Contribution to the project: collaboration   
More detail: domestic

Organization Name: Northwestern  
Organization Location: Illinois, USA  
Partner’s Contribution to the project: collaboration   
More detail: domestic

Organization Name: the Center for Hierarchical Materials Design (CHiMaD)  
Organization Location: Illinois, USA  
Partner’s Contribution to the project: collaboration   
More detail: domestic

Organization Name: the Massachusetts Institute of Technology (MIT)  
Organization Location: Massachusetts, USA  
Partner’s Contribution to the project: collaboration   
More detail: domestic

Organization Name: the University of Illinois at Urbana-Champaign  
Organization Location: Illinois, USA  
Partner’s Contribution to the project: collaboration   
More detail: domestic

Organization Name: the University of Wisconsin-Madison  
Organization Location: Illinois, USA  
Partner’s Contribution to the project: collaboration   
More detail: domestic

Organization Name: Globus  
Organization Location: Illinois, USA  
Partner’s Contribution to the project: collaboration, personnel exchange  
More detail: domestic

Organization Name: 3M  
Organization Location: Illinois, USA  
Partner’s Contribution to the project: collaboration, personnel exchange  
More detail: domestic

**36\. Have other collaborators or contacts been involved?**  
Other collaborators have included M.W. Keller (UTulsa) to create a SiC epoxy composite database, Sam Blau (Lawrence Berkeley National Lab), Andrew Rosen (Princeton), Matt Moderwell (Ouro Foundation) as an open source contributor to the superconductor database, the NSF APTO Project of James Evans (UChicago) “Global Observatory and Virtual Laboratory for Science and Technology Advance” for co-development of the metadata extraction pipeline.

We have continued our data engagements with the key partners listed above, and continue to provide all of the MDF services to thousands of researchers in the materials community.

# Impact

**37\. What was the impact on the development of the principal discipline(s) of the project?**  
This project delivers new data services that enable thousands of researchers in the materials science community to access services that simplify the publication, sharing, discovery, and exchange of high-value scientific datasets. With the Materials Data Facility (MDF), researchers have e.g., the ability to publish and discover datasets of unprecedented size (i.e., many TB) where other publication platforms allow only much smaller datasets (e.g., a few GB). In this project, we also seek to extend the existing Materials Data Facility services to include new capabilities to handle streams of data from instrumentation and simulation and to make datasets easier to plug together for re-use and meta analysis.

In Phase IV, our exploration of the usage of LLMs combined with core data infrastructure will serve as an exemplar for other disciplines. We expect the capabilities we develop here to be broadly generalizable to other domains e.g, chemistry, biology, engineering, and more.

**38\. What was the impact on other disciplines?**  
Capabilities developed, supported, and maintained via the Materials Data Facility project are currently in use in domains ranging from materials science to X-ray science, high energy physics, and more. Further, the Automate service initially created and developed by MDF has been moved into Globus production availability, and is now available to 500,000 users of Globus across all scientific domains, and across the world.

We believe that, while MDF is focused on materials science, our advances in data infrastructure and AI integration provide a template for other scientific disciplines seeking to modernize their data sharing platforms. In phase IV, we will showcase a technical framework for processing scientific content with LLMs, generating educational materials, and more that could be adapted for use in fields ranging from chemistry to physics, biology, and engineering. Additionally, our approach to more simply providing ML-ready datasets to the community could benefit any field working with machine learning applications.

**39\. What was the impact on the development of human resources?**  
In September 2025, we developed training materials and hosted a virtual hackathon to encourage students, postdocs, and other researchers to develop new applications in materials and chemistry using large language models, especially with data from MDF and other NIST sources. The hackathon attracted nearly 1400 registrants and 120 participating teams from across the world. In Phase III, we operated two combined hackathons that reached \~800 students, and researchers from national labs and industry.

**40\. What was the impact on teaching and educational experiences?**  
In this period, we continued working with a number of students as interns, and engaged a broader set of students through our continued hackathon events and papers.

**MDF has been used to create a course module:** “Machine Learning in Materials Science: Image Analysis Using Convolutional Neural Networks in MatCNN” by Tiberiu Stan, Jim James, Nathan Pruyne, Marcus Schwarting, Jiwon Yeom, Peter Voorhees, Ben Blaiszik, Ian Foster, Jonathan Emery available on NanoHub and already accessed by 2453 users (last accessed May 2025).

This course introduces fundamental concepts of artificial intelligence within the context of materials science and image segmentation. The two-week module was taught as part of a Computational Methods in Materials Science course at Northwestern University. The module is aimed at upper-level undergraduate and graduate students with basic materials science and computer programming knowledge. The course is composed of six lectures, three laboratory exercises, and a machine learning based image segmentation software termed MatCNN. 

**41\. What was the impact on physical, institutional, and information resources that form infrastructure?**  
MDF served as a key user of the Petrel storage cluster testbed at Argonne National Laboratory. Based on feedback from MDF and other users, the 100 PB Eagle cluster was commissioned for community usage. MDF is a key user of the Illinois NCSA Taiga storage cluster, and serves to help Illinois NCSA understand scientific use cases for large datasets. The Eagle cluster has been used to make the OMol25 Electronic Structures dataset available to the community, and also for assembling and eventually making available the 1 PB MOF dataset.

**42\. What was the impact on technology transfer?**  
Nothing to report

**43\. What was the impact on society beyond science and technology?**  
With a dramatically larger collection of data from researchers, we expect to enable researchers to bring to bear new analysis, discovery, and AI methods to discover new materials at a significantly accelerated pace. Such accelerated discovery is key to the Materials Genome Initiative goals, and may have societal impact by creation of new industries. Areas of particular interest include discovery of new quantum materials to unlock quantum computing capabilities, energy storage materials to improve electric vehicles and grid robustness, semiconductor materials for next generation microchips, polymers with low toxicity profiles for medical applications, and more. 

**44\. What percentage of the award’s budget was spent in foreign country(ies)?**  
None

# Changes/Problems

**45\. Changes in approach and reasons for change**  
Nothing to report

**46\. Actual or anticipated problems or delays and actions or plans to resolve them**  
Nothing to report

**47\. Changes that had a significant impact on expenditures**  
Nothing to report

**48\. Significant changes in use or care of human subjects, vertebrate animals, biohazards, and/or select agents**  
N/A

**49\. Change of primary performance site location from that originally proposed**  
Nothing to Report
