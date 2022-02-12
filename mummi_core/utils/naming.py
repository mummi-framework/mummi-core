# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
import os
import sys
import yaml
import errno
import time
from typing import Union
import datetime

from .utilities import mkdir
from .utilities import expand_path


# ------------------------------------------------------------------------------
class MuMMI_NamingUtils(object):

    # this dictionary contains the names of resource directories
    RESNAMES = {
        'martini':   '{resource}/martini',
        'charmm':    '{resource}/charmm',
        'ddcMD':     '{resource}/ddcmd',
        'states':    '{resource}/states-ras/',
        'rdfs':      '{resource}/rdfs-initial/ras-lipid',
        'agg_counts':'{resource}/rdfs-initial/aggregated_counts',
        'ml':        '{resource}/ml',
        'pmfcode':   '{resource}/pmfcode',
        'backmapping': '{resource}/backmapping',
        'structures': '{resource}/structures',
    }

    # this dictionary contains the names of directories in MuMMI_MUMMI_ROOT
    DIRNAMES = {
        'ml':          '{root}/ml',
        'redis':       '{root}/redis',
        'flux':        '{root}/flux',
        'workspace':   '{root}/workspace',
        #
        'macro':       '{root}/macro',
        'patches':     '{root}/patches',
        #
        'cg':          '{root}/sims-cg/{simname}',
        'aa':          '{root}/sims-aa/{simname}',
        #'cg-failed':   '{root}/sims-cg/failed/{simname}',
        #'aa-failed':   '{root}/sims-aa/failed/{simname}',
        #
        'feedback-cg': '{root}/feedback-cg2macro',
        'feedback-aa': '{root}/feedback-aa2cg'
    }

    # this dictionary contains the patterns of filenames
    FILENAMES = {
        #'grid':     'cfield.iter{iteration}#000000',
        #'hycop':    'snapshot.{snapshot:08d}',
        #
        #'pfpatch':  '{simname}_{idx:012d}',
        #'cgframe':  '{simname}_f{frame:012d}',
        #
        'snapshot': 'pos.{frame:012d}',
        'aaframe':  '{simname}_aaf{aaf:08d}',
        #'aaframe':  '{simname}_cgf{cgf:08d}_aaf{aaf:08d}',
        #
        #'cgpatch':  'alldists_sr2_pfpatch_{idx:012d}_cg',
        #'cgpatch':  '{simname}_{prefix}_f{idx:012d}',
        #
        #'eq-pdb':   '{simname}_{prefix}_eq_pdb',
        #'pdb':      '{simname}_{prefix}_f{idx:012d}_pdb',
        #'analysis': '{simname}_{prefix}_f{idx:012d}_a{atype}',
        #
        'structures':  '{pro_type}/{pro_type}_state_{state}_{struc_number}.gro',
        'rdfs':         'KRAS-{lipid}.state{state}.txt',
        'feedback_pdb': '{key}_{region}_{id}.pdb',
        'feedback_itp': '{key}_{id}.itp'
    }

    # STATUS_FLAGS
    STATUS_FLAGS = {
        'createsim':    ('createsims_success', 'createsims_failure'),
        'backmapping':  ('backmapping_success', 'backmapping_failure'),
        'cg':           ('cg_success', 'cg_failure'),
        'aa':           ('aa_success', 'aa_failure')
    }

    MUMMI_ROOT = ''
    RESOURCES = ''
    SPECS = ''
    CONFIG = {}

    # --------------------------------------------------------------------------
    # --------------------------------------------------------------------------
    @classmethod
    def init(cls, root='', resources='', app=''):

        # ----------------------------------------------------------------------
        # check the environment!
        if root == '':
            root = os.path.expandvars(os.environ['MUMMI_ROOT'])

        if resources == '':
            resources = os.path.expandvars(os.environ['MUMMI_RESOURCES'])

        if app == '':
            app = os.path.expandvars(os.environ['MUMMI_APP'])

        # ----------------------------------------------------------------------
        # TODO: get the specs from arguments
        cls.MUMMI_ROOT = expand_path(root)
        cls.MUMMI_RESOURCES = expand_path(resources)
        cls.MUMMI_SPECS = expand_path(os.path.join(app, 'specs'))

        if not os.path.isdir(cls.MUMMI_RESOURCES):
            raise ValueError(f'MuMMI_Resources ({cls.MUMMI_RESOURCES}) does not exist!')

        if not os.path.isdir(cls.MUMMI_SPECS):
            raise ValueError(f'MuMMI_Specs ({cls.MUMMI_SPECS}) does not exist!')

        if not os.path.isdir(cls.MUMMI_ROOT):
            mkdir(cls.MUMMI_ROOT)

        cls.CONFIG = {}

        # ----------------------------------------------------------------------
        configfile = os.path.join(cls.MUMMI_SPECS, 'workflow', 'mummi.yaml')
        with open(os.path.abspath(configfile)) as data:
            cls.CONFIG = yaml.full_load(data)

        macro_ml = cls.CONFIG['ml']['macro'].get('path', None)
        if not macro_ml:
            cls.CONFIG['ml']['macro']['path'] = cls.dir_res('ml')

        #cg_ml = cls.CONFIG['ml']['cg'].get('path', None)
        #if not cg_ml:
        #    cls.CONFIG['ml']['cg']['path'] = cls.dir_res('ml')

        # ----------------------------------------------------------------------
        print('> Initialized MuMMI\n' +
              '\t> MuMMI_ROOT: ({})\n\t> MuMMI_Resources: ({})\n\t> MuMMI_Specs: ({})\n'
              .format(cls.MUMMI_ROOT, cls.MUMMI_RESOURCES, cls.MUMMI_SPECS))

        # ----------------------------------------------------------------------

    # --------------------------------------------------------------------------
    # --------------------------------------------------------------------------
    # create the organization hierarchy at the root level
    @classmethod
    def create_root(cls, simname=None):
        print('> Creating MuMMI root hierarchy at ({})\n'.format(cls.MUMMI_ROOT))

        # ----------------------------------------------------------------------
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        config = {'created_on': st,
                  'root': cls.MUMMI_ROOT,
                  'resources': cls.MUMMI_RESOURCES,
                  'specs': cls.MUMMI_SPECS
                 }

        for key, val in sorted(cls.DIRNAMES.items()):
            try:
                if '{simname}' != val[-9:]:
                    p = val.format(root=cls.MUMMI_ROOT)
                elif simname is None:
                    p = val[:-9].format(root=cls.MUMMI_ROOT)
                else:
                    p = val.format(root=cls.MUMMI_ROOT, simname=simname)

                mkdir(p)
                config[key] = p
            except KeyError as e:
                pass

        cpath = os.path.join(cls.MUMMI_ROOT, 'config.yaml')

        if not os.path.isfile(cpath):
            with open(cpath, 'w') as outfile:
                yaml.dump(config, outfile, default_flow_style=False)

    # --------------------------------------------------------------------------
    # file names for macro model
    #@classmethod
    #def fname_hycop(cls, snapshot):
    #    return cls.FILENAMES['hycop'].format(snapshot=snapshot)

    #@classmethod
    #def fname_grid(cls, iteration):
    #    return cls.FILENAMES['grid'].format(iteration=iteration)

    #@classmethod
    #def pdb(cls, simname, prefix, fidx):
    #    # this will change when we move to actual pdb file pattern!
    #    return cls.FILENAMES['pdb'].format(simname=simname, prefix=prefix,idx=fidx)

    #@classmethod
    #def eq_pdb(cls, patchid, cg_id):
    #    return cls.FILENAMES['eq-pdb'].format(simname=patchid, prefix=cg_id)

    #@classmethod
    #def analysis(cls, simname, prefix, frame_idx, atype):
    #    return cls.FILENAMES['analysis'].format(simname=simname, \
    #                                     prefix=prefix, idx=frame_idx, atype=atype)

    # --------------------------------------------------------------------------
    @classmethod
    def pfpatch(cls, frame_idx, simname='pfpatch'):
        return f'{simname}_{frame_idx:012d}'

    @classmethod
    def cgframe(cls, simname, frame_idx):
        return f'{simname}_f{frame_idx:012d}'

    @classmethod
    def pfpatch_parse(cls, patch_id):
        idx = patch_id.rfind('_')
        return patch_id[0:idx], int(patch_id[idx + 1:])

    @classmethod
    def cgframe_parse(cls, frame_id):
        idx = frame_id.rfind('_')
        return frame_id[0:idx], int(frame_id[idx + 2:])

    # --------------------------------------------------------------------------
    @classmethod
    def fname_rdfs(cls, lipid, state):
        return cls.FILENAMES['rdfs'].format(lipid=lipid, state=state)

    @classmethod
    def protein_structure(cls, protein_type, state, structure_number):
        return cls.FILENAMES['structures'].format(pro_type=protein_type, state=state, struc_number=structure_number)

    @classmethod
    def snapshot(cls, frame_id):
        return cls.FILENAMES['snapshot'].format(frame=frame_id)

    @classmethod
    def aaframe(cls, simname, aaf):
        return cls.FILENAMES['aaframe'].format(simname=simname, aaf=aaf)

    @classmethod
    def fname_fb_pdb(cls, key, region, id):
        return cls.FILENAMES['feedback_pdb'].format(key=key, region=region, id=id)

    @classmethod
    def fname_fb_itp(cls, key, id):
        return cls.FILENAMES['feedback_itp'].format(key=key, id=id)

    @staticmethod
    def _format_fb_key(value: Union[str, int]):
        if isinstance(value, int):
            str_fmt = f'{value:08d}'
        elif isinstance(value, str):
            str_fmt = \
                f'{int(value):08d}' if value != '*' else f'{value}'
        else:
            raise TypeError(
                f'"Value provided as type({type(value)}) -- only types '
                '"str" and "int" expected. Value={value}')

        return str_fmt

    @classmethod
    def fb_macro_key(cls, key_type: str, simname: str = '*',
        nreports: Union[str, int] = '*'):

        if key_type not in ['worker', 'manager']:
            raise ValueError(
                f'"key_type={key_type}" not supported! Expected "manager" or '
                '"worker".')

        base_name = f'rdf_sim_{simname}' if key_type == 'worker' else 'rdf_main'
        idx_name = cls._format_fb_key(nreports)
        return f'{base_name}_{idx_name}'

    @classmethod
    def fb_macro_key_extract_simname(cls, key: str):

        # the above function formats it as follows
        # rdf_sim_{simname}_{frame}

        # first get rid of prefix and extn
        prefix = 'rdf_sim_'
        nprefix = len(prefix)

        extn = '.npz'
        nextn = len(extn)

        assert key[:nprefix] == prefix
        assert key[-nextn:] == extn

        sim = key[nprefix:-nextn]

        # now we still need to get rid of the frame number
        idx = sim.rfind('_')
        return sim[:idx]

    @classmethod
    def fb_aa_key_ras4braf(cls, key_type: str, simname: str = 'rasraf*',
        frame: Union[str, int] = '*', nreports: Union[str, int] = '*'):

        if key_type not in ['worker', 'manager']:
            raise ValueError(
                f'"key_type={key_type}" not supported! Expected "manager" or '
                '"worker".')

        nreport_str = cls._format_fb_key(nreports)
        frame_str = cls._format_fb_key(frame)

        ret_str = \
            f'{simname}_aaf{frame_str}' if key_type == 'worker' \
            else f'aafb_main_{nreport_str}'

        return ret_str

    @classmethod
    def fb_aa_key_ras4araf(cls, key_type: str, simname: str = 'ras4araf*',
        frame: Union[str, int] = '*', nreports: Union[str, int] = '*'):

        if key_type not in ['worker', 'manager']:
            raise ValueError(
                f'"key_type={key_type}" not supported! Expected "manager" or '
                '"worker".')

        nreport_str = cls._format_fb_key(nreports)
        frame_str = cls._format_fb_key(frame)

        ret_str = \
            f'{simname}_aaf{frame_str}' if key_type == 'worker' \
            else f'aafb_main_{nreport_str}'

        return ret_str


    @classmethod
    def fb_aa_key_ras4b(cls, key_type: str, simname: str = 'ras_*',
        frame: Union[str, int] = '*', nreports: Union[str, int] = '*'):

        if key_type not in ['worker', 'manager']:
            raise ValueError(
                f'"key_type={key_type}" not supported! Expected "manager" or '
                '"worker".')

        nreport_str = cls._format_fb_key(nreports)
        frame_str = cls._format_fb_key(frame)

        ret_str = \
            f'{simname}_aaf{frame_str}' if key_type == 'worker' \
            else f'aafb_main_{nreport_str}'

        return ret_str

    @classmethod
    def fb_aa_key_ras4a(cls, key_type: str, simname: str = 'ras4a_*',
        frame: Union[str, int] = '*', nreports: Union[str, int] = '*'):

        if key_type not in ['worker', 'manager']:
            raise ValueError(
                f'"key_type={key_type}" not supported! Expected "manager" or '
                '"worker".')

        nreport_str = cls._format_fb_key(nreports)
        frame_str = cls._format_fb_key(frame)

        ret_str = \
            f'{simname}_aaf{frame_str}' if key_type == 'worker' \
            else f'aafb_main_{nreport_str}'

        return ret_str

    #def aaframe(cls, simname, cgf, aaf):
    #   return cls.FILENAMES['aaframe'].format(simname=simname, cgf=cgf, aaf=aaf)

    #@classmethod
    #def cgpatch(cls, idx):
    #    return cls.FILENAMES['cgpatch'].format(idx=idx)

    # @classmethod
    # def cgpatch(cls, simname, prefix, frame_idx):
        # return cls.FILENAMES['cgpatch'].format(simname=simname, prefix=prefix, idx=frame_idx)

    #@classmethod
    #def cgpatch2pfpatch(cls, cgpatch):
    #    #ASSUMPTION: pfPatch name was used as simname in cgPatch
    #    return cgpatch.split('_')[0]

    # --------------------------------------------------------------------------
    # get directory names
    @classmethod
    def dir_root(cls, key, root=''):
        if root == '':
            root = cls.MUMMI_ROOT
        try:
            return cls.DIRNAMES[key].format(root=root)
        except KeyError as e:
            raise KeyError('Invalid directory name ({}) requested!'.format(key))

    @classmethod
    def atl_dir_sim(cls, key, root, simname):
        try:
            return cls.DIRNAMES[key].format(root=root, simname=simname)
        except KeyError as e:
            raise KeyError('Invalid directory name ({}) requested!'.format(key))

    @classmethod
    def dir_sim(cls, key, simname=''):
        return cls.atl_dir_sim(key, cls.MUMMI_ROOT, simname)

    @classmethod
    def dir_res(cls, key):
        try:
            return cls.RESNAMES[key].format(resource=cls.MUMMI_RESOURCES)
        except KeyError as e:
            raise KeyError('Invalid resource name ({}) requested!'.format(key))

    # --------------------------------------------------------------------------
    @classmethod
    def status_flags(cls, job_type):
        return cls.STATUS_FLAGS[job_type]

    @classmethod
    def get_mu_from_simname(cls, simname):
        s = simname.split('-')
        if len(s[0]) == 2:
            s = '-'.join([s[0], s[1]])
        else:
            s = s[0]
        return int(s[2:])

    # --------------------------------------------------------------------------
    @classmethod
    def dir_local(cls, subdir=''):
        if subdir == '':
             return cls.CONFIG['local_dir']
        else:
             ts = time.time()
             st = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')
             return os.path.join(cls.CONFIG['local_dir'], f'{subdir}-{st}')

    @classmethod
    def ml(cls, key=''):
        if len(key) == 0:
            return cls.CONFIG['ml']
        else:
            return cls.CONFIG['ml'][key]

# ------------------------------------------------------------------------------
