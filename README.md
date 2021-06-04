# MongoDB_Modelling_Querying
Java implementation based on modelling documents to insert them in the Document Store, based on the upcoming queries that are expected to be performed.

Using the MongoDB Java API, we explore in this project different modelling alternatives in MongoDB, which benefits or worsens specific types of queries. The conceptual model this project follows is the following one:
![imagen](https://user-images.githubusercontent.com/69221572/120863897-aab44900-c58b-11eb-954e-a0ee24fb5379.png)

The queries that are implemented in this project are always these 4:
**Q1:** For each person, retrieve their full name and their company’s name.
**Q2:** For each company, retrieve its name and the number of employees.
**Q3:** For each person born before 1988, update their age to “30”.
**Q4:** For each company, update its name to include the word “Company”.

These 4 queries will be checked in the following 3 document structures:
**M1:** Two types of documents, one for each class and referenced fields.
**M2:** One document for “Person” with “Company” as embedded document.
**M3:** One document for “Company” with “Person” as embedded documents.


**Conclusions that arise from the performance of each query in each of the document models:**
As we have seen, a given document design might be perfectly suitable for a specific query, but getting poor performance with another different query. The design heavily depends on the queries we expect to have.

Overall, we must always pursuit a balance between maintenance and throughput. In other words, a balance between the number of queries from our workload we are able to perform with a given document structure without denormalizing so much that we start to get redundancy, and the throughput we get with such queries at the expense of such redundancy. So, sometimes, it requires creating different collections that suit better to different groups of queries, as long as the throughput we obtain makes absolutely worthwhile to assume some redundancy due to the existence of the same values among collections or even documents from the same collection.

In short, we must make the most of the avoidance of Joins (main bottleneck of RDBMS) that Document Stores provide by applying the aforementioned denormalization, which clearly speeds up our workload of queries, if the document structures are well designed and match perfectly the queries.

However, in terms of the ‘updates’, they clearly suffer from the redundancy that denormalization usually entails. So, the more redundancy you have in your database (either among your collections or among documents from the same collection), the more expensive the maintenance is going to be (you will get very low throughput when updating the documents from your database). Thus, we must know how frequent the updates are going to be in our database, in order to assess the level of redundancy we ought to permit. Otherwise, the maintenance would be infeasible and the Document Store starts to lose its benefits.

Consequently, denormalizing usually entails redundancy, especially in one-to-many or many-to-many relationships among the concepts we are modelling. So, denormalizing must be applied with care in order to avoid a huge decrease in the overall throughput, depending on the queries we are dealing with, and being also important for both read and write (updates) operations, in our Document Store, trying always to find the aforementioned balance between maintenance and throughput.

