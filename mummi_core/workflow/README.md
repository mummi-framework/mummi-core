# Workflow

MuMMI workflow communicates with the [Flux Scheduler](https://github.com/flux-framework/flux-sched) 
through the `JobTracker` class, utilizing a scheduling API provided by scheduler adapters in the 
[Maestro Workflow Conductor](https://github.com/LLNL/maestrowf).

To add a new job, we must write a JobTracker 
config to specify its prerequisites and behavior. The initialization of these 
configs is to be handled by the MuMMI App (e.g., `mummi_ras`).

Example JobTracker config:

```yaml
job_type: aa

config:
  bundle_size: 1

  jobbin:     aaanalysis
  jobname:    aaanalysis
  jobdesc:    AAAnalysis ({}).

  nnodes:     1
  nprocs:     1
  ngpus:      1
  cores per task: 3
  
  use_broker: True
  addtl_args:
    mpi: spectrum

imports: # currently, we require importing the mummi_app package
  - mummi_ras

variables:
  simname: null # value supplied by jobTracker
  timestamp: null # value supplied by jobTracker
  locpath:
    - eval: mummi_ras.Naming.dir_local()
    - /aa-{timestamp}

  outpath:
    eval: mummi_ras.Naming.dir_sim('aa', '{simname}')

  outfile: 'aa_analysis.out'


script: |
  mkdir -p {outpath}
  mkdir -p {locpath}; cd {locpath}

  $(LAUNCHER) sh -c aa_analysis \
    --fstype mummi \
    --fbio mummi \
    --fbpath $MUMMI_ROOT/feedback-aa \
    --simname {simname} \
    --fcount 0 \
    --step 1 \
    >> {outfile} 2>&1
  
  wait
```

#### Explanation:

- `job_type`: Used for marking logs.
- `config`: Specification of the requirements of the job. These are passed to 
  `JobTracker.__init__()`.
- `imports:` Useful if an `eval` is included in the `variables`'` section, 
  and some module needs to be imported for that evaluation.
- `variables`
	- can be accessed with `{<variable_name>}` syntax like with python format strings.
	- two special variables are provided, which can be used in other variables 
	  and the final script
		- `simname`: the name of a simulation to be run by the next available 
		  compute resource
		- `timestamp`: the current timestamp
	- these symbols will become available to use in subsequent variable declarations and in the script value.
		- e.g., a symbol `locpath` is defined in `variables` and accessed in 
		  the script as: `{locpath}`
	- `list`: if a list is provided as a value, the list items will be concatenated with a: `''.join([<vals>])`
	- `eval`: if a value is something like: `{eval: 'mummi_ras.get("my_path")'}`, the value of eval will be evaluated as a python string
- `script`: variables are substituted into this string, and the final value 
  is used to generate the submission script

