# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------

def flux_environment():

    env_variables = []
    '''[
    ['FLUX_JOB_ID'],
    ['PMI_FD','PMI_RANK','PMI_SIZE'],
    ['PMIX_ID','PMIX_RANK','PMIX_NAMESPACE','PMIX_PTL_MODULE','PMIX_INSTALL_PREFIX',
     'PMIX_SERVER_TMPDIR','PMIX_SECURITY_MODE','PMIX_DSTORE_ESH_BASE_PATH'],
    ['MPI_ROOT'],
    ['OMPI_UNIVERSE_SIZE','OMPI_COMM_WORLD_SIZE','OMPI_COMM_WORLD_RANK',
     'OMPI_COMM_WORLD_NODE_RANK','OMPI_COMM_WORLD_LOCAL_RANK','OMPI_COMM_WORLD_LOCAL_SIZE',
     'OMPI_FIRST_RANKS','OMPI_NUM_APP_CTX','OMPI_COMMAND','OMPI_ARGV','OMPI_FILE_LOCATION'],
    ['OMPI_MCA_pml','OMPI_MCA_pmix','OMPI_MCA_ess','OMPI_MCA_ess_base_jobid',
     'OMPI_MCA_ess_base_vpid','OMPI_MCA_opal_signal','OMPI_MCA_initial_wdir',
     'OMPI_MCA_coll_hcoll_enable','OMPI_MCA_mpi_yield_when_idle',
     'OMPI_MCA_mca_base_env_list_distro','OMPI_MCA_mca_base_component_show_load_errors',
     'OMPI_MCA_shmem_RUNTIME_QUERY_hint','OMPI_MCA_schizo_ompi_prepend_ld_library_path',
     'OMPI_MCA_orte_launch','OMPI_MCA_orte_hnp_uri','OMPI_MCA_orte_app_num',
     'OMPI_MCA_orte_num_nodes','OMPI_MCA_orte_tmpdir_base','OMPI_MCA_orte_ess_num_procs',
     'OMPI_MCA_orte_ess_node_rank','OMPI_MCA_orte_bound_at_launch',
     'OMPI_MCA_orte_top_session_dir','OMPI_MCA_orte_jobfam_session_dir',
     'OMPI_MCA_orte_precondition_transports'],
    ['OPAL_PREFIX','OPAL_LIBDIR','OPAL_OUTPUT_STDERR_FD',
     'OMPI_LD_PRELOAD_POSTPEND_DISTRO'],
    ['SMPI_HCOLL_ENABLE_BCAST','SMPI_HCOLL_ENABLE_GATHER',
     'SMPI_HCOLL_ENABLE_ALLTOALLV','SMPI_HCOLL_ENABLE_IALLTOALL']
    ]
    '''

    cmd = ''
    for V in env_variables:
        cmd = cmd + 'unset '
        for v in V:
            cmd = cmd + '{} '.format(v)
        cmd = cmd + '\n'
    '''
    cmd += 'echo \"flux:\" `which flux`\n'
    cmd += 'echo \"-----------------------------------------------------\"\n'
    cmd += 'echo `flux --version`\n'
    cmd += 'echo \"-----------------------------------------------------\"\n'
    cmd += 'echo `flux module list`\n'
    cmd += 'echo \"-----------------------------------------------------\"\n'
    '''
    return cmd


def flux_command():
    return "flux mini run -N {} -n {} -c {} -o \"mpi=spectrum\" sh -c"


def flux_uri():

    import os
    try:
        from mummi_core import Naming
        fname = os.path.join(Naming.dir_root('flux'), 'flux.info')
        with open(fname) as fp:
            return fp.read().strip()

    except Exception as e:
        print('error in getting flux uri: {}'.format(e))
    return ''
