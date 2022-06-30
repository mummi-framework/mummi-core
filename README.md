## MuMMI Core v1.0
#### Released: Jun 29, 2022


<b>MuMMI Core</b> is the underlying infrastructure and generalizable component of the <b>MuMMI framework</b>,
which facilitates the coordination of massively parallel multiscale simulations.

MuMMI was developed as part of the <b><i>Pilot2</b></i> project of the
[Joint Design of Advanced Computing Solutions for Cancer](https://cbiit.cancer.gov/ncip/hpc/jdacs4c)
funded jointly by the [Department of Energy](http://www.doe.gov) (DOE) and the [National Cancer Institute](http://www.cancer.gov) (NCI).


The Pilot 2 project focuses on developing multiscale simulation models for
understanding the interactions of the lipid plasma membrane with the [RAS and RAF](https://www.cancer.gov/research/key-initiatives/ras)
proteins. The broad computational tool development aims of this pilot are:
* Developing scalable multi-scale molecular dynamics code that will automatically switch between continuum, coarse-grained and all-atom simulations.
* Developing scalable machine learning and predictive models of molecular simulations to:
    * identify and quantify states from simulations
    * identify events from simulations that can automatically signal change of resolution between continuum, coarse-grained and all-atom simulations
    * aggregate information from the multi-resolution simulations to efficiently feedback to/from machine learning tools
* Integrate sparse information from experiments with simulation data.


MuMMI Core exposes abstract functionalities that allow writing the simulation
components easily. It is designed to be used in conjunction with a "MuMMI App",
which defines the simulation components as well as specifies specific configurations
for MuMMI Core. MuMMI Core contains different I/O interfaces, workflow
abstractions (job trackers and feedback managers), as well as several additional
utilities.

#### Publications

The MuMMI framework is described in the following publications. Please make appropriate
citations to relevant papers.

##### Workflow

1. Bhatia et al. <b>Generalizable Coordination of Large Multiscale Ensembles: Challenges and Learnings at Scale</b>.
  In Proceedings of the ACM/IEEE International Conference for High Performance Computing, Networking, Storage and Analysis, SC '21,
  Article No. 10, November 2021.
  [doi:10.1145/3458817.3476210](https://doi.org/10.1145/3458817.3476210).

2. Di Natale et al. <b>A Massively Parallel Infrastructure for Adaptive Multiscale Simulations: Modeling RAS Initiation Pathway for Cancer</b>.
  In Proceedings of the ACM/IEEE International Conference for High Performance Computing, Networking, Storage and Analysis, SC '19, Article No. 57, November 2019.
  [doi:10.1145/3295500.3356197](https://doi.org/10.1145/3295500.3356197).
  <br/><b><i>Best Paper at SC 2019</i></b>.


#### Requirements
- Python 3.7
- A compatible MuMMI application (e.g. MuMMI RAS)

#### Installation
```
git clone https://github.com/mummi-framework/mummi-core
cd mummi-core
pip3 install .
```

#### Usage

MuMMI core exposes several command line utilities.

- `mummi_monitor`: Monitor all running simulations.
- `mummi_cmdserver`: Setup a command server (for the primary user) to allow
  other users to query the job status.
- `mummi_cmdclient`: Command client for the queries enabled by the server.
- `mummi_bind_global_redis`: Bind to the redis cluster
  (uses environment to fetch host and port).
- `mummi_bind_local_redis`: Bind to one redis node within the redis cluster
  (uses environment to fetch host and port).

Most of the usage comes through a python interface, by importing `mummi_core`
to leverage the utilities provided here.

#### Authors and Acknowledgements

MuMMI Core was developed at Lawrence Livermore National Laboratory
and the main contributors are:

Harsh Bhatia, Joseph Y Moon, Francesco Di Natale, Joseph R Chavez,
James Glosli, and Helgi I Ingólfsson.

MuMMI was funded by the Pilot 2 project led by Dr. Fred Streitz (DOE) and
Dr. Dwight Nissley (NIH). We acknowledge contributions from the entire
Pilot 2 team.

This work was performed under the auspices of the U.S. Department
of Energy (DOE) by Lawrence Livermore National Laboratory under Contract DE-AC52-07NA27344.

Contact: Lawrence Livermore National Laboratory, 7000 East Avenue, Livermore, CA 94550.

#### Contributing

Contributions may be made through pull requests and/or issues on github.

### License

MuMMI Core is distributed under the terms of the MIT License.

Livermore Release Number: LLNL-CODE-827197
