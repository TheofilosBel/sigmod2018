# ACM SIGMOD PROGRAMMING CONTEST 2018

## _"Dataurus"_

### Team

* Belmpas Theofilos <sdi1400119@di.uoa.gr>, University of Athens, Department of Informatics and Telecommunications, B.Sc. student
* Chalatsis Dimitrios <sdi1400219@di.uoa.gr>, University of Athens, Department of Informatics and Telecommunications, B.Sc. student
* Gkini Orest <sdi1400036@di.uoa.gr>, University of Athens, Department of Informatics and Telecommunications, B.Sc. student
* Kamaras Georgios <sdi1400058@di.uoa.gr>, University of Athens, Department of Informatics and Telecommunications, B.Sc. student
* Michas Georgios <sdi1400109@di.uoa.gr>, University of Athens, Department of Informatics and Telecommunications, B.Sc. student
* Svingos Christoforos <csvingos@di.uoa.gr>, University of Athens, Department of Informatics and Telecommunications, Ph.D. student

### Supervisors

* Professor Yannis Ioannidis <yannis@di.uoa.gr>, University of Athens

### Acknowledgements

* Nikolopoulos Evangelos <vgnikolop@di.uoa.gr>, University of Athens, Department of Informatics and Telecommunications

## About

This **Join queries** project was developed at **Spring of 2018** for the **ACM SIGMOD Programming Contest 2018**. For more information on the contest, please visit [the contest's website](http://sigmod18contest.db.in.tum.de/index.shtml).

### A brief explanation of our solution

Each time a batch of queries is received, each query is parsed, cleaned from weak filters and an
optimal join tree is created to guide the query execution in the most efficient way. In the end of the batch,
the queries are sorted based on a cost estimation and are scheduled for execution accordingly.
This way they are executed in parallel in an efficient way and the checksums of their results are collected.
For the joins, the _Radix Join_ algorithm utilized. The checksums are then printed in the order that the queries came.

Several cache optimizations and optimizations based on statistics collected during each query's preprocessing phase
have been applied during the above process. Also, a Job Scheduler has been used to assist in the optimal parallelization
of critical parts of our program, like checksum calculation and filter application.

## Third party code

* [Radix Join](https://www.systems.ethz.ch/node/334) (MIT License)
