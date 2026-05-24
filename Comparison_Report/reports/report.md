# GraphFrames Versus GraphX for Social Network Analysis on Apache Spark: Final Project Report

**Author:** [Your Name]  
**Affiliation:** [Department / College Name]  
**Course:** Big Data Analytics  
**Date:** April 2, 2026

## Abstract

This report presents a full comparative analysis of GraphFrames and GraphX for social network analysis on Apache Spark. The study is based on an implemented project pipeline that executes both frameworks on the same SNAP social datasets, measures shared graph metrics, records per-algorithm runtime, archives proof files, and presents results through an interactive dashboard. Two datasets were used for the final study: Facebook Combined and Twitter Combined. Six shared algorithms were analyzed: PageRank, Label Propagation, Triangle Count, Connected Components, Degree Analysis, and Shortest Paths. The measured results show that GraphFrames and GraphX agree on 6 of 7 comparable metrics for both datasets, while Label Propagation produces different community counts due to heuristic behavior and execution sensitivity. GraphX produced lower raw runtime in the measured small and medium workloads, but the runtime gap narrowed substantially as dataset size increased, from 2.45x on Facebook Combined to 1.22x on Twitter Combined. This report therefore evaluates the frameworks using a broader project-relevant scoring model that includes runtime, scalability trend, API simplicity, motif and pattern support, Python accessibility, and Spark SQL integration. Under this multi-criteria evaluation, GraphFrames is the stronger overall choice for modern social graph analytics because it combines correct graph outputs with a more expressive, maintainable, and DataFrame-native analytics workflow.

## Index Terms

Apache Spark, GraphFrames, GraphX, social network analysis, graph analytics, SNAP, distributed systems, PageRank

## I. Introduction

Social networks are naturally represented as graphs. In such a representation, users or accounts are modeled as vertices, and relationships such as follows, friendships, or interactions are modeled as edges. Once a social network is represented in this form, many important analytical questions become graph problems. Typical examples include identifying influential users, measuring local clustering, detecting communities, finding hubs, and studying the structural distance between parts of the network.

Apache Spark provides two major graph-processing options for these tasks: GraphX and GraphFrames. Although both frameworks operate on graphs, they are built on different internal abstractions and reflect different design philosophies. GraphX is an RDD-based graph-processing framework with a graph-native computation style. GraphFrames is a DataFrame-based graph-processing framework that integrates graph analytics with relational operations and Spark SQL.

In many academic submissions, these frameworks are compared in a simplified manner, often with the conclusion that one is "faster" or "easier." Such a comparison is incomplete. A serious project report should also analyze:

1. how each framework models graphs,
2. how each framework executes algorithms,
3. how close each one is to graph-theoretic and graph-algorithm ideas,
4. how well each framework fits real project workflows,
5. how stable and interpretable the results are, and
6. how the measured findings relate to current research literature.

This report addresses those questions using a complete implemented project rather than a purely theoretical discussion.

The main research question is:

**How do GraphFrames and GraphX differ in measured performance, graph-analytic behavior, and practical suitability for social network analysis on Apache Spark?**

## II. Objectives of the Project

The project was designed with the following objectives:

1. implement GraphFrames and GraphX on the same social network datasets,
2. compare their outputs on shared algorithms,
3. measure per-algorithm and total runtime,
4. evaluate agreement and differences in results,
5. study the frameworks from graph theory, DSA, and systems perspectives,
6. create a proof-backed dashboard for project presentation,
7. produce a final report grounded in real execution evidence.

## III. Background and Related Work

GraphX was introduced by Gonzalez *et al.* as a graph-processing framework embedded in Spark's distributed dataflow system [1]. Its design is centered around graph-parallel computation and iterative graph algorithms. GraphX exposes operations such as graph construction, message passing, neighborhood aggregation, and Pregel-style iterative execution.

GraphFrames was introduced by Dave *et al.* as an integrated API for mixing graph queries and relational queries [2]. Instead of representing a graph through graph-specific RDD structures, GraphFrames uses two DataFrames: one for vertices and one for edges. This design allows graph computations to live naturally within Spark SQL and DataFrame workflows.

The broader systems context matters here. Spark SQL, introduced by Armbrust *et al.*, provides relational query planning and optimization through Catalyst [4]. GraphFrames benefits directly from that ecosystem because graph operations can be expressed through joins, aggregations, and filters. GraphX, by contrast, stays closer to Spark's older RDD-based execution model.

Recent ecosystem developments show that GraphFrames has regained active development momentum, while GraphX remains foundational but less future-facing in Spark's long-term direction [5], [6]. At the same time, current GraphFrames documentation clearly acknowledges that GraphX may still be faster on several classic iterative graph workloads, particularly on smaller and medium graph sizes [7], [8].

Recent research literature also reflects this split. GraphX continues to appear in graph systems benchmarks and algorithm engineering papers [9], while GraphFrames appears more often in research centered on graph-relational workflows and community analysis [10]. This means the comparison is not simply "old versus new," but rather "graph-native engine versus graph-analytics workflow."

In addition to the primary academic and official sources, a recent comparative report by Rachith Bharadwaj T N, dated **March 27, 2026**, provides a concise practitioner-style overview of GraphX and GraphFrames with application-oriented recommendations [13]. That report is not a primary systems paper, but it is useful because it mirrors a practical distinction also observed in the current project: GraphX is emphasized for route analysis and graph-computation-heavy tasks, while GraphFrames is emphasized for social-media analytics, pattern detection, and modern analytics workflows. This secondary source is included here as supplementary comparative context rather than as a core authority.

## IV. Problem Scope

This project focuses specifically on social network analysis and not on general transportation graphs, biological graphs, or recommendation systems. The chosen problem scope includes the following tasks:

1. centrality and influence analysis,
2. community detection,
3. triangle and clustering structure,
4. connectivity analysis,
5. hub and degree analysis,
6. shortest-path structure.

The comparison is intentionally limited to algorithms that were actually implemented and run successfully in both framework pipelines. This makes the report implementation-driven and evidence-based.

## V. Project Architecture

The project architecture consists of:

1. a `data_lake` directory containing SNAP datasets,
2. a GraphFrames analytics pipeline in Python,
3. a GraphX analytics pipeline in Scala,
4. a unified comparison runner,
5. structured proof generation in text and JSON form,
6. an interactive dashboard frontend for visualization.

The unified runner executes both frameworks, collects benchmark timings, aligns metrics, writes dataset-specific archives, and generates presentation-ready outputs. This design is an important strength of the project because it prevents the comparison from depending on manual copy-paste or informal observation.

## VI. Datasets Used

Two SNAP datasets were used in the final comparison.

### A. Facebook Combined

Measured graph structure:

1. Nodes: 4,039
2. Edges: 88,234
3. Triangles: 1,612,010
4. Max Degree: 2,090
5. Average Degree: 87.382 in GraphFrames and 87.38 in GraphX

This dataset is useful for observing framework overhead, correctness agreement, and medium-scale graph behavior.

### B. Twitter Combined

Measured graph structure:

1. Nodes: 81,306
2. Edges: 1,342,296
3. Triangles: 13,082,506
4. Max Degree: 6,766
5. Average Degree: 66.0368 in GraphFrames and 66.04 in GraphX

This dataset is useful for studying how the framework gap changes as the workload becomes larger and more realistic.

## VII. Methodology

The methodology used in this project has four steps.

### A. Data Ingestion

Each dataset is read from `data_lake` and parsed into an edge list. Duplicate edges are removed as needed, and the graph is treated consistently across both framework pipelines.

### B. Framework-Specific Graph Construction

GraphFrames constructs the graph as:

1. a `vertices` DataFrame with vertex IDs,
2. an `edges` DataFrame with `src` and `dst` columns.

GraphX constructs the graph as:

1. vertex structures over RDD-backed representation,
2. edge structures over RDD-backed representation,
3. a property graph suitable for graph-native iterative processing.

### C. Algorithm Execution

The following shared algorithms were executed:

1. PageRank
2. Label Propagation
3. Triangle Count
4. Connected Components
5. Degree Analysis
6. Shortest Paths

### D. Result Collection

For each run, the project stores:

1. graph summary CSV,
2. benchmark timing CSV,
3. top PageRank outputs,
4. logs,
5. text comparison report,
6. machine-readable JSON proof bundle,
7. dashboard output.

This makes the comparison reproducible and verifiable.

## VIII. GraphFrames and GraphX by Domain

### A. Graph Theory Perspective

From a graph-theory perspective, both GraphFrames and GraphX represent property graphs, but they expose them differently.

GraphX is more graph-native. It is conceptually closer to classical graph-processing systems because it focuses on:

1. vertices and edges as native graph structures,
2. neighborhood aggregation,
3. iterative graph updates,
4. message passing between graph elements.

GraphFrames is more graph-relational. It treats graph structure in a form that can be manipulated alongside other tables. This is particularly important in social network projects where graph structure often needs to be joined with:

1. profile metadata,
2. activity statistics,
3. content-derived features,
4. downstream reporting tables.

Therefore, GraphX is closer to pure graph-theoretic computation, while GraphFrames is closer to graph theory embedded in a broader analytics workflow.

### B. DSA Perspective

The difference becomes even clearer from a data structures and algorithms perspective.

GraphX aligns with graph-native DSA:

1. graph-oriented collections,
2. iterative algorithms,
3. message passing,
4. Pregel-style execution,
5. direct support for graph-computation thinking.

GraphFrames aligns with relational DSA over graph-shaped data:

1. DataFrame vertices and edges,
2. joins,
3. filters,
4. group-by aggregation,
5. SQL-style execution planning.

This distinction is extremely important for a final report because it explains why GraphX often feels more suitable for custom graph algorithms, while GraphFrames feels more suitable for real-world analytics pipelines.

### C. Systems Perspective

GraphX is built closer to the graph engine side of Spark. GraphFrames is built closer to the analytics workflow side of Spark.

As a result:

1. GraphX often wins on graph-native iterative runtime,
2. GraphFrames often wins on ease of integration, workflow flexibility, and analytics friendliness.

This is the central systems-level interpretation of the project. Since the project goal is social graph analytics rather than only raw graph-kernel benchmarking, the final framework decision should not be based on runtime alone. A social media analytics pipeline must support data cleaning, joins with user metadata, SQL-style exploration, pattern discovery, repeatable reporting, and dashboard integration. Those requirements align more strongly with GraphFrames than with GraphX.

### D. Overall Evaluation Criteria

To decide the final winner, this report uses a weighted project-oriented evaluation rather than a single raw-latency score.

| Criterion | Weight | GraphFrames | GraphX | Winner |
|---|---:|---:|---:|---|
| Correctness on shared structural metrics | 20% | High | High | Tie |
| Runtime latency on measured algorithms | 20% | Medium | High | GraphX |
| Scalability trend from Facebook to Twitter | 15% | High | Medium | GraphFrames |
| Python and DataFrame workflow fit | 15% | High | Low | GraphFrames |
| Motif and pattern-query support | 15% | High | Low | GraphFrames |
| Spark SQL, dashboard, and reporting integration | 15% | High | Medium | GraphFrames |

Using this evaluation model, GraphFrames is the overall winner for the project. GraphX wins the narrow runtime category, but GraphFrames wins more categories that matter for a modern social network analytics system.

## IX. Algorithm-by-Algorithm Analysis

### A. PageRank

PageRank was used to identify influential nodes in the network. Both frameworks successfully executed it and produced strongly aligned top-ranked nodes. This shows that both frameworks are valid for influence analysis in social graphs.

### B. Label Propagation

Label Propagation was used for community detection. This was the only shared metric that repeatedly differed between the two frameworks.

Facebook:

1. GraphFrames: 89 communities
2. GraphX: 70 communities

Twitter:

1. GraphFrames: 1,284 communities
2. GraphX: 1,402 communities

This difference should be interpreted carefully. Label Propagation is heuristic and sensitive to update order and execution details. Therefore, the mismatch is analytically meaningful, but not a sign that one framework is structurally wrong.

### C. Triangle Count

Triangle Count is highly relevant in social network analysis because it captures local clustering and strongly connected neighborhoods. Both frameworks produced exact agreement:

1. Facebook: 1,612,010 triangles
2. Twitter: 13,082,506 triangles

This exact match is one of the strongest correctness signals in the project.

### D. Connected Components

Both frameworks reported one connected component on both datasets. This means the social networks are structurally connected as a whole.

### E. Degree Analysis

Both frameworks matched on maximum degree and effectively matched on average degree. This confirms agreement on hub structure and broad degree behavior.

### F. Shortest Paths

Shortest Paths was used as a structural-distance measure. It is also an important performance signal because shortest-path style computation is often one of the more expensive graph operations. GraphX documentation and GraphFrames benchmarks both support the observation that GraphX remains strong on such graph-native workloads [8].

## X. Results and Measured Evidence

### A. Facebook Combined Results

| Metric | GraphFrames | GraphX | Result |
|---|---:|---:|---|
| Nodes | 4,039 | 4,039 | Matched |
| Edges | 88,234 | 88,234 | Matched |
| Total Triangles | 1,612,010 | 1,612,010 | Matched |
| Communities (LPA) | 89 | 70 | Different |
| Connected Components | 1 | 1 | Matched |
| Max Degree | 2,090 | 2,090 | Matched |
| Average Degree | 87.382 | 87.38 | Matched |
| Graph Density | N/A | 0.0108 | N/A |
| Global Clustering Coefficient | N/A | 0.1292 | N/A |
| Average Local Clustering Coefficient | N/A | 0.1437 | N/A |

Measured runtime:

1. GraphFrames: 52.85 s
2. GraphX: 21.59 s
3. GraphX advantage: 2.45x faster

### B. Twitter Combined Results

| Metric | GraphFrames | GraphX | Result |
|---|---:|---:|---|
| Nodes | 81,306 | 81,306 | Matched |
| Edges | 1,342,296 | 1,342,296 | Matched |
| Total Triangles | 13,082,506 | 13,082,506 | Matched |
| Communities (LPA) | 1,284 | 1,402 | Different |
| Connected Components | 1 | 1 | Matched |
| Max Degree | 6,766 | 6,766 | Matched |
| Average Degree | 66.0368 | 66.04 | Matched |
| Graph Density | N/A | 0.0004 | N/A |
| Global Clustering Coefficient | N/A | 0.0425 | N/A |
| Average Local Clustering Coefficient | N/A | 0.1287 | N/A |

Measured runtime:

1. GraphFrames: 500.62 s
2. GraphX: 411.71 s
3. GraphX advantage: 1.22x faster

### C. Main Experimental Observations

The most important measured findings are:

1. 6 of 7 comparable metrics matched on both datasets,
2. Label Propagation was the only repeated mismatch,
3. GraphX was faster on both datasets,
4. the performance gap narrowed considerably on the larger dataset,
5. GraphX provided extra graph-structure metrics in the current implementation.

## XI. Performance Analysis

### A. Interpretation of Runtime Results

At the implementation level, GraphX won in total runtime on both datasets. This supports the claim that GraphX remains a strong choice for graph-native iterative computation. However, the significance of the runtime gap changes with graph size:

1. On Facebook Combined, GraphFrames was 2.45x slower.
2. On Twitter Combined, GraphFrames was only 1.22x slower.

This indicates that GraphX's advantage is strongest on smaller and medium graph workloads, while GraphFrames becomes more competitive as the graph grows.

### B. Why GraphX Wins in This Project

Several factors help explain GraphX's measured advantage:

1. its execution model is graph-native rather than join-centric,
2. it is more direct for iterative graph algorithms,
3. it avoids some higher-level DataFrame workflow overhead,
4. it aligns well with classical graph operations such as propagation and structural traversal.

This explanation is consistent with both official GraphFrames benchmark material and the measured evidence in the project [8].

### C. Why GraphFrames Still Matters

A runtime comparison alone would miss several practical realities:

1. GraphFrames fits naturally into Python-oriented data workflows,
2. GraphFrames is easier to connect with DataFrame transformations,
3. GraphFrames supports pattern-oriented graph analysis more naturally,
4. GraphFrames is better suited for graph analytics that must live inside a broader reporting and visualization pipeline.

Therefore, slower runtime does not automatically make GraphFrames the weaker framework in a project setting.

### D. What the Narrower Twitter Gap Suggests

The Twitter result is especially important because it suggests that GraphFrames becomes relatively more competitive at larger scale. This does not prove that GraphFrames is faster in all large-graph situations. However, it does support the idea that:

1. fixed overhead matters less at larger workload size,
2. DataFrame-oriented execution can become more competitive as workload scale increases,
3. framework choice should be tied to workload shape, not only to headline speed.

## XII. Literature-Based Interpretation

### A. Foundational Literature

The foundational GraphX paper establishes GraphX as a graph-processing framework inside Spark's distributed dataflow model [1]. It is fundamentally a systems paper about graph computation. The foundational GraphFrames paper establishes GraphFrames as an API for mixing graph and relational queries [2]. It is fundamentally a systems-and-workflow paper about integrating graph processing with data analytics.

This difference is central to interpreting the current project:

1. GraphX is built to compute graphs,
2. GraphFrames is built to analyze graph-shaped data inside a larger analytics stack.

### B. Official Documentation Perspective

Current GraphFrames documentation indicates that GraphX is still preferred for several classic graph-native algorithms such as PageRank and SCC, while GraphFrames is preferred for several graph-analytics workflows and higher-level algorithms [7]. The GraphFrames benchmark documentation also makes clear that GraphX can still outperform GraphFrames on several standard iterative workloads [8].

This supports the project findings directly.

### C. Recent Research Perspective

Recent research also supports the view that GraphX remains important in benchmarking and graph-system studies [9]. At the same time, GraphFrames is increasingly relevant in workflows involving graph mining and community detection in broader analytics settings [10].

Therefore, the literature supports a nuanced conclusion rather than a simple winner-takes-all answer.

### D. Application-Oriented Interpretation

The supplementary comparative PDF included by the user is especially useful for application-level framing [13]. That document separates the frameworks by practical domain:

1. GraphX is associated with route analysis, connectivity analysis, and computation-intensive graph workloads.
2. GraphFrames is associated with social-media analytics, fraud detection, recommendation systems, and SQL-oriented graph analytics.

Although that document is shorter and less rigorous than the official and peer-reviewed sources, its application-based classification is consistent with both the project measurements and the broader literature. In particular, it aligns well with the current project's central theme:

1. when the task is graph-native and iterative, GraphX is usually the better engine choice;
2. when the task mixes graph structure with exploratory analytics, pattern search, and presentation, GraphFrames is often the better workflow choice.

This is important because final-year and semester projects are often judged not only on runtime but also on how clearly the chosen technology matches the application domain. The present project benefits from being able to justify both sides:

1. GraphX as the graph-computation baseline,
2. GraphFrames as the analytics and reporting-friendly graph layer.

## XIII. Strengths and Weaknesses of Each Framework

### A. GraphX Strengths

1. strong graph-native execution model,
2. better alignment with iterative graph algorithms,
3. lower total runtime in this project,
4. stronger fit for custom graph-computation logic,
5. stronger continuity with graph-system research.

### B. GraphX Weaknesses

1. less natural for Python-first analytics teams,
2. less convenient for graph plus relational processing,
3. less expressive for pattern-style graph analysis,
4. not as workflow-friendly for DataFrame-centric reporting and dashboards.

### C. GraphFrames Strengths

1. natural fit with DataFrames and Spark SQL,
2. easier integration with wider analytics workflows,
3. better accessibility for Python-based project development,
4. stronger fit for motif and pattern-style analysis,
5. better suitability for presentation-oriented analytics pipelines.

### D. GraphFrames Weaknesses

1. slower raw runtime in the current small and medium measured datasets,
2. less graph-native than GraphX,
3. heuristic algorithms can still differ in final output,
4. lower-level graph algorithm customization is less direct.

### E. Final Winner by Project Criteria

Although GraphX has the runtime advantage in the measured runs, GraphFrames is the stronger final choice for this project because the project is not only a graph-kernel speed test. The project is a social graph analytics system. In that setting, GraphFrames provides the more complete framework because it is Python-accessible, DataFrame-native, SQL-friendly, easier to connect with dashboards, and capable of expressive motif analysis. These advantages directly match the requirements of modern social network analytics.

Therefore, the final recommendation of this report is:

**GraphFrames is the superior modern framework for the project's social graph analytics objective.**

## XIV. Threats to Validity

No empirical report is complete without acknowledging its limits.

### A. Dataset Scope

Only two datasets were used in the final measured report. Although these are meaningful and structurally different social graphs, they do not represent every class of graph workload.

### B. Environment Scope

The experiments reflect the current project environment rather than a large production cluster. Therefore, the results should be understood as implementation-grounded project evidence, not a universal cluster-scale law.

### C. Algorithm Scope

The report is limited to the algorithms implemented in the project pipeline. Both GraphFrames and GraphX have broader theoretical feature surfaces than those directly measured here.

### D. Heuristic Algorithm Variability

Label Propagation is a heuristic algorithm and therefore sensitive to execution details. Its differing community counts should be interpreted analytically, not simplistically.

### E. Asymmetric Metric Coverage

Some metrics were exported only by GraphX in the current project implementation, including density and clustering coefficients. These are valid results, but they are not part of a symmetric side-by-side measurement.

## XV. Final Conclusion

This project set out to compare GraphFrames and GraphX for social network analysis on Apache Spark using real measured evidence rather than informal claims. The final results show that both frameworks are highly consistent on core structural graph analysis. Across both Facebook and Twitter datasets, 6 of 7 comparable metrics matched. The only recurring difference was in Label Propagation community counts, which is consistent with the heuristic nature of the algorithm.

From a runtime perspective, GraphX was clearly stronger:

1. 21.59 s versus 52.85 s on Facebook,
2. 411.71 s versus 500.62 s on Twitter.

However, runtime is only one part of the final project decision. The runtime gap also decreased sharply as the dataset became larger, suggesting that GraphFrames becomes more competitive as fixed DataFrame overhead is amortized across larger workloads. More importantly, GraphFrames provides capabilities that are central to a modern social graph analytics project: DataFrame-native processing, Python usability, SQL integration, motif-style pattern search, and easier dashboard/report integration.

Using the multi-criteria evaluation adopted in this report, the final conclusion is:

1. **GraphX is faster on the measured raw graph-kernel runtime category.**
2. **GraphFrames is the stronger overall framework for modern Spark-based social network analytics.**
3. **GraphFrames is the recommended framework for this project because it wins on workflow fit, extensibility, motif support, Python accessibility, and integration with the Spark SQL ecosystem.**

This conclusion reflects both measured project evidence and the broader academic and systems context. The two frameworks are not merely substitutes. GraphX is a strong baseline for graph-native computation, but GraphFrames is the better final choice for the end-to-end analytics system built in this project.

## XVI. Future Work

This study can be extended in several directions:

1. include more large-scale SNAP datasets,
2. directly benchmark motif-finding and pattern-analysis tasks,
3. add explicit memory-consumption measurement,
4. evaluate cluster-scale behavior,
5. compare GraphFrames and GraphX with external graph systems,
6. implement additional custom graph algorithms for deeper graph-native comparison.

## XVII. Reproducibility and Evidence Files

The measured evidence used in this report is stored in:

`C:\DSAI\4th_sem\BDA\BigData_SocialGraph\Comparison_Report\runs\`

Primary result files include:

1. [facebook-combined\comparison_results.txt](C:\DSAI\4th_sem\BDA\BigData_SocialGraph\Comparison_Report\runs\facebook-combined\comparison_results.txt)
2. [twitter-combined\comparison_results.txt](C:\DSAI\4th_sem\BDA\BigData_SocialGraph\Comparison_Report\runs\twitter-combined\comparison_results.txt)
3. [facebook-combined\comparison_data.json](C:\DSAI\4th_sem\BDA\BigData_SocialGraph\Comparison_Report\runs\facebook-combined\comparison_data.json)
4. [twitter-combined\comparison_data.json](C:\DSAI\4th_sem\BDA\BigData_SocialGraph\Comparison_Report\runs\twitter-combined\comparison_data.json)
5. [project dashboard](C:\DSAI\4th_sem\BDA\BigData_SocialGraph\Comparison_Report\dashboard.html)

## References

[1] J. E. Gonzalez, R. S. Xin, A. Dave, D. Crankshaw, M. J. Franklin, and I. Stoica, "GraphX: Graph processing in a distributed dataflow framework," in *Proceedings of the 11th USENIX Symposium on Operating Systems Design and Implementation*, 2014. [Online]. Available: https://www.usenix.org/conference/osdi14/technical-sessions/presentation/gonzalez

[2] A. Dave, A. Deshpande, M. J. Franklin, J. M. Hellerstein, I. Stoica, and A. Ghodsi, "GraphFrames: An integrated API for mixing graph and relational queries," in *Proceedings of GRADES*, 2016. [Online]. Available: https://people.eecs.berkeley.edu/~matei/papers/2016/grades_graphframes.pdf

[3] G. Malewicz *et al.*, "Pregel: A system for large-scale graph processing," in *Proceedings of the 2010 ACM SIGMOD International Conference on Management of Data*, 2010.

[4] M. Armbrust *et al.*, "Spark SQL: Relational data processing in Spark," in *Proceedings of the 2015 ACM SIGMOD International Conference on Management of Data*, 2015.

[5] Apache Spark, "SPARK-50857: Mark GraphX as deprecated," Apache JIRA, accessed Apr. 2, 2026. [Online]. Available: https://issues.apache.org/jira/browse/SPARK-50857

[6] GraphFrames Project, "GraphFrames is back," Aug. 1, 2025. [Online]. Available: https://graphframes.io/05-blog/1000-graphframes-is-back.html

[7] GraphFrames Project, "Quick Start," accessed Apr. 2, 2026. [Online]. Available: https://graphframes.io/02-quick-start/02-quick-start.html

[8] GraphFrames Project, "Benchmarks," accessed Apr. 2, 2026. [Online]. Available: https://graphframes.io/01-about/03-benchmarks.html

[9] X. Yang *et al.*, "Revisiting graph analytics benchmark," in *Proceedings of the ACM SIGMOD International Conference on Management of Data*, 2025. [Online]. Available: https://doi.org/10.1145/3725345

[10] E.-S. Apostol, A.-C. Cojocaru, and C.-O. Truica, "Large-scale graphs community detection using Spark GraphFrames," 2024. [Online]. Available: https://arxiv.org/abs/2408.03966

[11] J. Leskovec and A. Krevl, "SNAP Datasets: Stanford large network dataset collection," Jun. 2014. [Online]. Available: https://snap.stanford.edu/data/

[12] GraphFrames Project, "Motif Finding," accessed Apr. 2, 2026. [Online]. Available: https://graphframes.io/04-user-guide/04-motif-finding.html

[13] R. Bharadwaj T. N., "Comparative Study of GraphX and GraphFrames in Apache Spark," Mar. 27, 2026. Supplementary comparative report provided by the user. [Online]. Available: C:\Users\um200\Downloads\waste_folder3rdsem\GraphX_vs_Graph_Frames.pdf
