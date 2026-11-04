import atexit
import dataclasses
import json
import os
import re
import shutil
import subprocess
import sys
from urllib.parse import quote

from pandaclient import MiscUtils, PandaToolsPkgInfo, PrunArgs
from pandaclient.CommonArgs import (
    VALID_TRANSFER_TYPES,
    check_input_options,
    check_pfn_list_options,
    check_scouting_options,
    check_split_options,
    get_bulk_io_list,
    get_invalid_transfer_types,
    load_json_options,
    parse_secondary_datasets,
    process_common_options,
    process_sandbox_options,
    set_n_files_from_n_jobs,
)
from pandaclient.task_param_utils import (
    get_allow_no_output_job_params,
    get_container_job_params,
    get_dbr_job_params,
    get_unmatched_allow_no_output_patterns,
    set_build_task_params,
    set_common_task_params,
    set_good_run_list_input_params,
    set_log_task_params,
    set_merge_task_params,
    set_pfn_list_input_params,
    set_prepro_task_params,
    set_source_url,
    use_build_step,
)


def _copy_root_core_packages(options, cur_dir, files_to_delete_on_exit, tmp_log):
    """Copy the user's RootCore packages and the RootCore grid scripts into the sandbox, for --useRootCore

    RootCore was the build system of the Run 2 ROOT-based analysis releases, before the CMake-based ones.
    The packages are copied into <cur_dir>/__panda_rootCoreWorkDir, which is shipped in the sandbox.
    On the worker node, buildGen sources grid_compile.sh (or grid_compile_nobuild.sh) to compile them,
    and runGen and runMerge source grid_run.sh to set up the environment before running --exec.

    Appends to options.extFile and to files_to_delete_on_exit, and exits with EC_Config on errors.

    args:
        options: parsed prun options
        cur_dir: directory where prun was started, which is under the work directory
        files_to_delete_on_exit: files and directories deleted when prun exits
        tmp_log: logger
    """
    # $ROOTCOREDIR is set by the RootCore setup, the scripts come from that installation
    if "ROOTCOREDIR" not in os.environ:
        tmp_log.error("$ROOTCOREDIR is not defined in your environment. Please setup RootCore runtime beforehand")
        sys.exit(MiscUtils.EC_Config)

    # grid_submit*.sh copy the packages on the client, grid_compile*.sh build them on the worker node
    # and grid_run.sh sets up the environment of the jobs. The nobuild variants are for --noBuild
    root_core_submit_sh = f"{os.environ['ROOTCOREDIR']}/scripts/grid_submit.sh"
    root_core_compile_sh = f"{os.environ['ROOTCOREDIR']}/scripts/grid_compile.sh"
    root_core_run_sh = f"{os.environ['ROOTCOREDIR']}/scripts/grid_run.sh"
    root_core_submit_nb_sh = f"{os.environ['ROOTCOREDIR']}/scripts/grid_submit_nobuild.sh"
    root_core_compile_nb_sh = f"{os.environ['ROOTCOREDIR']}/scripts/grid_compile_nobuild.sh"

    # old RootCore versions don't have all the scripts
    root_core_sh_list = [root_core_submit_sh, root_core_compile_sh, root_core_run_sh]
    if options.noBuild:
        root_core_sh_list.append(root_core_submit_nb_sh)
    # copied below whenever --noCompile is used, with or without --noBuild
    if options.noCompile:
        root_core_sh_list.append(root_core_compile_nb_sh)
    for tmp_sh_file in root_core_sh_list:
        if not os.path.exists(tmp_sh_file):
            tmp_log.error(f"{tmp_sh_file} doesn't exist. Please use a newer version of RootCore")
            sys.exit(MiscUtils.EC_Config)
    tmp_log.info("copy RootCore packages to current dir")

    # the worker-node scripts expect this directory name under the run directory
    panda_root_core_work_dir_name = "__panda_rootCoreWorkDir"
    root_core_dest_work_dir = f"{cur_dir}/{panda_root_core_work_dir_name}"
    # ship everything in it, including the files skipped by default (.o, .a, .so, .root, large files)
    options.extFile.append(f"{panda_root_core_work_dir_name}/.*")
    # the copy is only needed for the sandbox
    files_to_delete_on_exit.append(root_core_dest_work_dir)

    # copy the packages, without building them for --noBuild
    if options.noBuild:
        submit_sh = root_core_submit_nb_sh
    else:
        submit_sh = root_core_submit_sh
    # the output of the script goes to the terminal, as before with os.system
    tmp_stat = subprocess.run([submit_sh, root_core_dest_work_dir]).returncode
    if tmp_stat != 0:
        tmp_log.error(f"{submit_sh} failed with {tmp_stat}")
        sys.exit(MiscUtils.EC_Config)

    # ship the scripts used on the worker node
    shutil.copy(root_core_run_sh, root_core_dest_work_dir)
    shutil.copy(root_core_compile_sh, root_core_dest_work_dir)
    if options.noCompile:
        shutil.copy(root_core_compile_nb_sh, root_core_dest_work_dir)


def _archive_athena_packages(options, cur_dir, tmp_dir, run_dir, work_area, group_area):
    """Archive the packages of the Athena work area into the sandbox tarball, for --useAthenaPackages

    Creates <tmp_dir>/sources.<uuid>.tar, or jobO.<uuid>.tar with --noBuild when CMake isn't used.
    The files under the work directory are appended to the same tarball afterwards.
    Changes the working directory, and exits on errors.

    args:
        options: parsed prun options
        cur_dir: directory where prun was started
        tmp_dir: temporary directory where the tarball is made
        run_dir: run directory relative to the work area
        work_area: Athena work area
        group_area: Athena group area, or "" when not set
    returns:
        (name of the uncompressed tarball, its full path)
    """
    # imported here as in main: importing Client resolves the panda cache host
    from pandaclient import AthenaUtils

    archive_name = ""
    # CMake setup ($CMAKE_PREFIX_PATH): cpack packs the build directory (the compiled packages under usr/)
    # into a plain tarball, empty if the build has no CPackConfig.cmake. prun always asks for the "sources"
    # name here, so the next steps append to sources.<uuid>.tar also with --noBuild
    if AthenaUtils.useCMake():
        archive_name, archive_full_name = AthenaUtils.archiveWithCpack(True, tmp_dir, options.verbose)
    # make AthenaUtils.matchExtFile use the --extFile patterns. The list is shared, not copied,
    # so it includes the RootCore directory added by _copy_root_core_packages
    AthenaUtils.setExtFile(options.extFile)
    if not options.noBuild:
        # the package sources, compiled by the build job on the grid. --gluePackages adds CMT glue packages
        archive_name, archive_full_name = AthenaUtils.archiveSourceFiles(
            work_area,
            run_dir,
            cur_dir,
            tmp_dir,
            options.verbose,
            options.gluePackages,
            dereferenceSymLinks=options.followLinks,
            archiveName=archive_name,
        )
    else:
        # --noBuild: no build job, only the job options and python modules (the files matching matchExtFile)
        archive_name, archive_full_name = AthenaUtils.archiveJobOFiles(
            work_area,
            run_dir,
            cur_dir,
            tmp_dir,
            options.verbose,
            archiveName=archive_name,
        )
    # add the InstallArea of the work area. The one of the group area is packed into groupArea.<uuid>.tar,
    # which is added to the tarball unless --noBuild is used
    AthenaUtils.archiveInstallArea(
        work_area,
        group_area,
        archive_name,
        archive_full_name,
        tmp_dir,
        options.noBuild,
        options.verbose,
    )
    return archive_name, archive_full_name


def _matches_any(patterns, path):
    """True if any of the regex patterns matches path"""
    return any(re.search(pattern, path) for pattern in patterns)


def _skip_reason(path, size, options, skipped_ext):
    """
    Why a file under the work dir is not sent to WNs

    :return: message to print, or None if the file is sent. Files matching --extFile are always sent
    """
    if _matches_any([f"{ext}$" for ext in options.extFile], path):
        return None
    if path.endswith(tuple(skipped_ext)):
        return f"skip {skipped_ext} {path}"
    if re.search(r"\.root(\.\d+)*$", path):
        return f"skip root file {path}"
    if size > options.maxFileSize:
        return f"skip large file {path}:{size}B>{options.maxFileSize}B"
    return None


def _walk_candidate_files(arch_start_dir, follow_links, exclude_patterns, athena_archive_name, verbose):
    """
    Walk arch_start_dir and yield (path, size) for readable files not matching exclude_patterns,
    and (path, None) for empty directories. Files named athena_archive_name are ignored
    """
    for root, dirs, files in os.walk(arch_start_dir, followlinks=bool(follow_links)):
        if not dirs and not files:
            yield root, None
        for file_name in files:
            path = f"{root}/{file_name}"
            if athena_archive_name and file_name == athena_archive_name:
                # the tarball is usually under the work dir, since tmp_dir is made in the current dir
                if verbose:
                    print(f"skip Athena archive {file_name}")
                continue
            try:
                size = os.path.getsize(path)
            except OSError as exc:
                # dead symlink
                if verbose:
                    print(f"  Ignore : {type(exc)}:{exc}")
            else:
                if not _matches_any(exclude_patterns, path):
                    yield path, size


def _gather_work_dir_files(options, arch_start_dir, tmp_dir, athena_archive_name, exclude_patterns, tmp_log):
    """Collect the files and empty directories under arch_start_dir to put in the sandbox, without the leading ./"""
    skipped_ext = [] if options.noCompile else [".o", ".a", ".so"]
    tmp_dir_name = tmp_dir.split("/")[-1]
    work_dir_files = []
    skipped_any = False
    walk = _walk_candidate_files(arch_start_dir, options.followLinks, exclude_patterns, athena_archive_name, options.verbose)
    for path, size in walk:
        # the --extFile patterns are matched against the path with the leading ./
        rel_path = re.sub(r"^\./", "", path)
        if size is None:
            # empty directory, except tmp_dir
            if not _matches_any(exclude_patterns, rel_path) and rel_path.split("/")[-1] != tmp_dir_name:
                work_dir_files.append(rel_path)
        elif reason := _skip_reason(path, size, options, skipped_ext):
            print(f"  {reason}")
            skipped_any = True
        else:
            work_dir_files.append(rel_path)
    if skipped_any:
        tmp_log.info("please use --extFile if you need to send the skipped files to WNs")
    return work_dir_files


def create_sandbox_archive(options, cur_dir, tmp_dir, run_dir, work_area, group_area, files_to_delete_on_exit, tmp_log):
    """Archive the user files into the sandbox tarball

    Copies the RootCore packages with --useRootCore, archives the Athena packages with --useAthenaPackages,
    adds the files under the work directory, then compresses and checks the tarball.
    Changes the working directory, ending in tmp_dir, appends to options.extFile and to files_to_delete_on_exit,
    and exits on errors.

    args:
        options: parsed prun options
        cur_dir: directory where prun was started
        tmp_dir: temporary directory where the tarball is made
        run_dir: run directory relative to the work area
        work_area: Athena work area, used only with --useAthenaPackages
        group_area: Athena group area, used only with --useAthenaPackages
        files_to_delete_on_exit: files and directories deleted when prun exits
        tmp_log: logger
    returns:
        name of the gzipped tarball in tmp_dir
    """
    # imported here as in main: importing Client resolves the panda cache host
    from pandaclient import AthenaUtils, PsubUtils

    if options.useRootCore:
        _copy_root_core_packages(options, cur_dir, files_to_delete_on_exit, tmp_log)

    # without --useAthenaPackages the tarball is named after gathering the files below
    archive_name, archive_full_name = "", ""
    if options.useAthenaPackages:
        archive_name, archive_full_name = _archive_athena_packages(options, cur_dir, tmp_dir, run_dir, work_area, group_area)

    # gather normal files
    if options.useAthenaPackages:
        os.chdir(work_area)
        tmp_log.info(f"gathering files under {work_area}/{run_dir}")
        arch_start_dir = run_dir.rstrip("/")
    else:
        os.chdir(options.workDir)
        tmp_log.info(f"gathering files under {options.workDir}")
        arch_start_dir = "."
    athena_archive_name = os.path.basename(archive_full_name) if options.useAthenaPackages else ""
    work_dir_files = _gather_work_dir_files(options, arch_start_dir, tmp_dir, athena_archive_name, AthenaUtils.excludeFile, tmp_log)

    # set archive name: 'jobO' for noBuild, 'sources' for normal build
    if not options.useAthenaPackages:
        prefix = "jobO" if options.noBuild and not options.noCompile else "sources"
        archive_name = f"{prefix}.{MiscUtils.wrappedUuidGen()}.tar"
        archive_full_name = f"{tmp_dir}/{archive_name}"

    # start one tar process for each entry in work_dir_files and append that entry to the same tarball.
    # A single tar over the directory would include all the files, also the ones that have been filtered out
    archive_base_name = os.path.basename(archive_full_name)
    for tmp_file in work_dir_files:
        if os.path.basename(tmp_file) == archive_base_name:
            # avoid self-archiving, e.g. the Athena archive
            if options.verbose:
                print(f"skip self-archiving for {tmp_file}")
        else:
            # append (-r) one entry at a time to the uncompressed tarball. Entries are files or empty directories.
            # --exclude '.[a-zA-Z]*' leaves out hidden files and directories (.git, .svn, editor files, ...)
            if os.path.islink(tmp_file):
                # -h dereferences the symlink: the tarball gets a copy of the target's content
                command = f"tar --exclude '.[a-zA-Z]*' -rh '{tmp_file}' -f '{archive_full_name}'"
            else:
                # regular file or empty directory: stored as is
                command = f"tar --exclude '.[a-zA-Z]*' -rf '{archive_full_name}' '{tmp_file}'"
            status, out = MiscUtils.commands_get_status_output(command)
            if options.verbose:
                print(tmp_file)
            if status != 0 or out != "":
                print(out)
    # go to tmpdir
    os.chdir(tmp_dir)

    # compress and check the sandbox
    return PsubUtils.finalize_sandbox(archive_name, options.verbose, tmp_log, check_symlinks=options.useAthenaPackages)


# main
def _join_args(conditional_args):
    """Join the arguments whose condition is true, each followed by a space as in the rest of the job parameters

    args:
        conditional_args: list of (condition, argument string)
    returns:
        joined arguments, "" when no condition is true
    """
    return "".join(f"{arg} " for condition, arg in conditional_args if condition)


def _has_cmt_config(options):
    return options.cmtConfig not in ["", "NULL", None]


def _container_multi_step_exec(options):
    """Steps to run the --exec string directly in the --containerImage container, set up by the pilot with ALRB"""
    multi_step_exec = {
        "preprocess": {"command": "${TRF}", "args": "--preprocess ${TRF_ARGS}"},
        "postprocess": {
            "command": "${TRF}",
            "args": "--postprocess ${TRF_ARGS}",
        },
        "containerOptions": {
            "containerExec": 'echo "=== cat exec script ==="; ' "cat __run_main_exec.sh; " "echo; " 'echo "=== exec script ==="; ' "/bin/sh __run_main_exec.sh",
            "containerImage": options.containerImage,
        },
    }
    if options.alrbArgs is not None:
        multi_step_exec["containerOptions"]["execArgs"] = options.alrbArgs
    return multi_step_exec


def _output_job_params(options, tmp_log):
    """Job parameters for the --outputs files

    Each output goes to the dataset <outDS without />_<suffix>/. The suffix is given as Suffix:FileName,
    or else it is the file name, or a sequence number for regex| outputs. Also warns about --allowNoOutput
    patterns that match no output.

    args:
        options: parsed prun options
        tmp_log: logger
    returns:
        list of job parameters, empty without --outputs. Exits with EC_Config on invalid output names
    """
    # imported here as in main: importing Client resolves the panda cache host
    from pandaclient import PsubUtils

    job_params = []
    if options.outputs != "":
        out_dataset_name = options.outDS
        out_map = {}
        ds_suffix = []
        ds_index = 0
        for tmp_lfn in options.outputs.split(","):
            tmp_ds_suffix = ""
            if ":" in tmp_lfn:
                tmp_ds_suffix, tmp_lfn = tmp_lfn.split(":")
                if tmp_ds_suffix in ds_suffix:
                    tmp_log.error(f"dataset name suffix '{tmp_ds_suffix}' is used for multiple files in --outputs. each output must have a unique suffix.")
                    sys.exit(MiscUtils.EC_Config)
                ds_suffix.append(tmp_ds_suffix)
            if tmp_lfn.startswith("regex|"):
                # regex
                lfn = tmp_lfn
                if not tmp_ds_suffix:
                    tmp_ds_suffix = ds_index
                    ds_index += 1
            else:
                tmp_new_lfn = tmp_lfn
                # change * to XYZ and add .tgz
                if "*" in tmp_new_lfn:
                    tmp_new_lfn = tmp_new_lfn.replace("*", "XYZ")
                    tmp_new_lfn += ".tgz"
                # disallowed character
                if "/" in tmp_new_lfn:
                    tmp_log.error(f"An output file name {tmp_new_lfn} contains '/'.")
                    sys.exit(MiscUtils.EC_Config)
                # check invalid characters
                checked = PsubUtils.check_invalid_char(tmp_new_lfn, is_file=True)
                if checked is not None:
                    tmp_log.error(f'An output file name {tmp_new_lfn} contains an invalid character "{checked}".')
                    sys.exit(MiscUtils.EC_Config)
                if len(out_dataset_name.split(".")) > 2:
                    lfn = "{}.{}".format(*out_dataset_name.split(".")[:2])
                else:
                    lfn = out_dataset_name[:-1]
                if options.addNthFieldOfInDSToLFN != "" or options.addNthFieldOfInFileToLFN != "":
                    lfn += "${MIDDLENAME}"
                lfn += f".$JEDITASKID._${{SN/P}}.{tmp_new_lfn}"
                if tmp_ds_suffix == "":
                    tmp_ds_suffix = tmp_new_lfn
            dataset = f"{out_dataset_name[:-1]}_{tmp_ds_suffix}/"
            job_params += MiscUtils.makeJediJobParam(
                lfn,
                dataset,
                "output",
                hidden=True,
                destination=options.destSE,
                token=options.spaceToken,
                allowNoOutput=options.allowNoOutput,
            )
            out_map[tmp_lfn] = lfn

        # runGen renames the files produced by --exec to the output file names
        job_params.append({"type": "constant", "value": f'-o "{out_map!s}"'})
        # output files allowed not to be produced
        job_params += get_allow_no_output_job_params(job_params)
    for tmp_patt in get_unmatched_allow_no_output_patterns(options.allowNoOutput, job_params):
        tmp_log.warning(
            f"--allowNoOutput pattern '{tmp_patt}' matches none of output filename templates "
            f"{[item['value'] for item in job_params if item.get('param_type') == 'output']}"
        )
    return job_params


def _input_ds_job_param(options, files_to_be_used):
    """Job parameter for the --inDS input dataset

    args:
        options: parsed prun options
        files_to_be_used: input file names from --inputFileList, empty to use all the files
    returns:
        template job parameter
    """
    in_ds_param = {
        "type": "template",
        "param_type": "input",
        "value": '-i "${IN/T}"',
        "dataset": options.inDS,
        "exclude": r"\.log\.tgz(\.\d+)*$",
    }
    if options.useLogAsInput:
        del in_ds_param["exclude"]
    if options.notExpandInDS:
        in_ds_param["consolidate"] = f"{'.'.join(options.outDS.split('.')[:2])}.{MiscUtils.wrappedUuidGen()}/"
    else:
        in_ds_param["expand"] = True
    if options.nSkipFiles != 0:
        in_ds_param["offset"] = options.nSkipFiles
    if options.match != "":
        in_ds_param["include"] = options.match
    if options.antiMatch != "":
        if "exclude" in in_ds_param:
            in_ds_param["exclude"] += f",{options.antiMatch}"
        else:
            in_ds_param["exclude"] = options.antiMatch
    if files_to_be_used != []:
        in_ds_param["files"] = files_to_be_used
    return in_ds_param


def _exec_job_params(options):
    """Job parameters for the --exec string, quoted as the -p argument of runGen"""
    # imported here as in main: importing Client resolves the panda cache host
    from pandaclient import PsubUtils

    return (
        [{"type": "constant", "value": '-p "', "padding": False}]
        + PsubUtils.convertParamStrToJediParam(options.jobParams, {}, "", True, False, includeIO=False)
        + [{"type": "constant", "value": '"'}]
    )


def _secondary_job_params(options):
    """Job parameters for the --secondaryDSs datasets

    returns:
        list of job parameters: one input template per secondary dataset, and --inMap giving runGen
        the files of each stream, the primary IN stream included
    """
    job_params = []
    in_map = {}
    stream_names = []
    if options.inDS != "":
        in_map["IN"] = "tmp_IN"
        stream_names.append("IN")
    for ds_name, secondary_ds in options.secondaryDSs.items():
        stream_name = secondary_ds["streamName"]
        job_params += MiscUtils.makeJediJobParam(
            f"${{{stream_name}}}",
            ds_name,
            "input",
            hidden=True,
            expand=not options.notExpandSecDSs,
            include=secondary_ds["pattern"],
            offset=secondary_ds["nSkip"],
            nFilesPerJob=secondary_ds["nFiles"],
            useNumFilesAsRatio=options.useNumFilesInSecDSsAsRatio,
            reusableAtt=stream_name in options.reusableSecondary,
            outDS=options.outDS,
            file_list=secondary_ds["files"],
        )
        in_map[stream_name] = f"tmp_{stream_name}"
        stream_names.append(stream_name)
    # replace the tmp_ values by the placeholders that JEDI fills with the files of each stream
    str_in_map = str(in_map)
    for stream_name in stream_names:
        str_in_map = str_in_map.replace(f"'tmp_{stream_name}'", f"${{{stream_name}/T}}")
    job_params.append({"type": "constant", "value": f'--inMap "{str_in_map}"'})
    return job_params


def _run_args(options):
    """Arguments for runGen in the run jobs, "" if none"""
    # imported here as in main: importing Client resolves the panda cache host
    from pandaclient import AthenaUtils

    container_without_alrb = options.containerImage != "" and not options.alrb
    args = _join_args(
        [
            (options.pfnList != "", "--givenPFN"),
            (options.useAthenaPackages, "--useAthenaPackages"),
            (AthenaUtils.useCMake(), "--useCMake"),
            (options.useRootCore, "--useRootCore"),
            (options.rootVer != "", f"--rootVer {options.rootVer}"),
            (_has_cmt_config(options), f"--cmtConfig {options.cmtConfig}"),
            (options.writeInputToTxt != "", f"--writeInputToTxt {options.writeInputToTxt}"),
            # debug parameters
            (options.queueData != "", f"--overwriteQueuedata={options.queueData}"),
            # exec string with real output filenames
            (options.execWithRealFileNames, "--execWithRealFileNames"),
        ]
    )
    args += get_container_job_params(options)
    args += _join_args(
        [
            (container_without_alrb and options.useCentralRegistry, "--useCentralRegistry=True"),
            (container_without_alrb and not options.useCentralRegistry and options.notUseCentralRegistry, "--useCentralRegistry=False"),
            (options.persistentFile, f"--fileToSave={options.persistentFile} --fileToLoad={options.persistentFile}"),
        ]
    )
    return args


def _build_args(options, run_dir):
    """Arguments for buildGen in the build job"""
    # imported here as in main: importing Client resolves the panda cache host
    from pandaclient import AthenaUtils

    build_job_parameters = "-i ${IN} -o ${OUT} --sourceURL ${SURL} " + f"-r {run_dir} "
    build_job_parameters += _join_args(
        [
            (options.bexec != "", f'--bexec "{quote(options.bexec)}"'),
            (options.useAthenaPackages, "--useAthenaPackages"),
            (options.useRootCore, "--useRootCore"),
            (options.noCompile, "--noCompile"),
            (AthenaUtils.useCMake(), "--useCMake"),
            (options.rootVer != "", f"--rootVer {options.rootVer}"),
            (_has_cmt_config(options), f"--cmtConfig {options.cmtConfig}"),
            # debug parameters
            (options.queueData != "", f"--overwriteQueuedata={options.queueData}"),
        ]
    )
    build_job_parameters += get_container_job_params(options)
    return build_job_parameters


def _merge_args(options, run_dir, archive_name):
    """Arguments for runMerge in the merge jobs"""
    # imported here as in main: importing Client resolves the panda cache host
    from pandaclient import AthenaUtils

    merge_job_parameters = f"-r {run_dir} "
    merge_job_parameters += _join_args(
        [
            (options.mergeScript != "", f'-j "{options.mergeScript}"'),
            (options.mergeSingleFile, "--mergeSingleFile"),
            (options.rootVer != "", f"--rootVer {options.rootVer}"),
            (_has_cmt_config(options), f"--cmtConfig {options.cmtConfig}"),
            (options.useAthenaPackages, "--useAthenaPackages"),
            (AthenaUtils.useCMake(), "--useCMake"),
            (options.useRootCore, "--useRootCore"),
        ]
    )
    # where the merge job gets the user files: the container, the build job's lib tarball, or the sandbox
    if options.containerImage != "" and not options.alrb:
        merge_job_parameters += get_container_job_params(options)
    elif use_build_step(options):
        merge_job_parameters += "-l ${LIB} "
    else:
        merge_job_parameters += f"-a {archive_name} --sourceURL ${{SURL}} "
    return merge_job_parameters


def _build_task_params(
    options,
    work_env,
    archive_name,
    included_site,
    full_exec_string,
    files_to_be_used,
    tmp_log,
):
    """
    Build the task parameters for JEDI from the prun options

    args:
        options: parsed prun options, already validated and normalized by main
        work_env: _WorkEnv with the run directory and the Athena setup
        archive_name: name of the uploaded sandbox, or None when no sandbox is sent
        included_site: sites given with --site
        full_exec_string: the full prun command line, stored in the task
        files_to_be_used: input file names from --inputFileList
        tmp_log: logger
    returns:
        task parameter map. Exits with EC_Config on invalid --outputs
    """
    # imported here as in main: importing Client resolves the panda cache host
    from pandaclient import AthenaUtils, Client

    task_param_map = {}
    if options.containerImage != "" and not options.alrb:
        processing_type = f"panda-client-{PandaToolsPkgInfo.release_version}-jedi-cont"
    else:
        processing_type = f"panda-client-{PandaToolsPkgInfo.release_version}-jedi-run"
    set_common_task_params(options, task_param_map, processing_type, included_site, full_exec_string, archive_name)
    if options.vo is None:
        task_param_map["vo"] = "atlas"
    else:
        task_param_map["vo"] = options.vo
    if options.containerImage != "" and options.alrb:
        task_param_map["architecture"] = options.architecture
    else:
        task_param_map["architecture"] = AthenaUtils.getCmtConfigImg(
            work_env.athena_version,
            work_env.cache_version,
            work_env.nightly_version,
            options.cmtConfig,
            architecture=options.architecture,
        )
    task_param_map["transUses"] = work_env.athena_version
    if work_env.athena_version != "":
        task_param_map["transHome"] = f"AnalysisTransforms{work_env.cache_version}{work_env.nightly_version}"
    else:
        task_param_map["transHome"] = None
    if options.msgDriven:
        task_param_map["messageDriven"] = True
    if options.workingGroup is not None:
        # remove role
        task_param_map["workingGroup"] = options.workingGroup.split(".")[0].split(":")[0]
    task_param_map["nMaxFilesPerJob"] = options.maxNFilesPerJob
    if options.useSecrets:
        task_param_map["useSecrets"] = True
    # source URL
    if options.vo is None:
        set_source_url(task_param_map, Client.baseURLCSRVSSL)
    else:
        set_source_url(task_param_map, Client.baseURLSSL)

    if options.containerImage != "" and options.alrb:
        task_param_map["container_name"] = options.containerImage
        if options.directExecInContainer:
            task_param_map["multiStepExec"] = _container_multi_step_exec(options)

    # log
    if not options.noSeparateLog:
        set_log_task_params(options, task_param_map)

    # job parameters, in the order runGen gets them
    job_params = [
        {"type": "constant", "value": '-j "" --sourceURL ${SURL}'},
        {"type": "constant", "value": f"-r {work_env.run_dir}"},
        {"type": "constant", "value": "__delimiter__", "hidden": True},
    ]
    task_param_map["jobParameters"] = job_params

    # where the run jobs get the user files: the sandbox with --noBuild, otherwise the build job's lib tarball
    if options.containerImage == "" or options.useSandbox:
        if options.noBuild and not options.noCompile:
            sandbox_arg = f"-a {archive_name}"
            if options.tarBallViaDDM:
                sandbox_arg += " --noTarballDownload"
        else:
            sandbox_arg = "-l ${LIB}"
        job_params.append({"type": "constant", "value": sandbox_arg})

    job_params += _output_job_params(options, tmp_log)

    # input
    if options.inDS != "":
        job_params.append(_input_ds_job_param(options, files_to_be_used))
        task_param_map["dsForIN"] = options.inDS
    elif options.pfnList != "":
        set_pfn_list_input_params(options, task_param_map)
    elif options.goodRunListXML != "":
        set_good_run_list_input_params(task_param_map)
    else:
        task_param_map["noInput"] = True

    job_params += _exec_job_params(options)
    job_params += get_dbr_job_params(options.dbRelease, options.notExpandDBR)

    if options.secondaryDSs != {}:
        job_params += _secondary_job_params(options)
        task_param_map["reuseSecOnDemand"] = True

    run_args = _run_args(options)
    if run_args != "":
        job_params.append({"type": "constant", "value": run_args})

    # force stage-in
    if options.forceStaged or options.transferType == "file":
        task_param_map["useLocalIO"] = 1
    elif options.useDirectIOSites or options.transferType == "direct":
        task_param_map["allowInputLAN"] = "only"

    # build step
    build_args = _build_args(options, work_env.run_dir) if use_build_step(options) else None
    set_build_task_params(options, task_param_map, archive_name, build_args)

    # preprocessing step
    set_prepro_task_params(options, task_param_map)

    # merging
    if options.mergeOutput:
        set_merge_task_params(options, task_param_map, _merge_args(options, work_env.run_dir, archive_name), tmp_log)
    return task_param_map


@dataclasses.dataclass
class _WorkEnv:
    """Where prun runs: the run directory and the Athena setup. The Athena fields are "" when not used"""

    # run directory relative to the work dir, or relative to the work area with a trailing / for --useAthenaPackages
    run_dir: str = "."
    # Athena release like Atlas-24.0.10, cache and nightly versions, from --athenaTag or --useAthenaPackages
    athena_version: str = ""
    cache_version: str = ""
    nightly_version: str = ""
    # only for --useAthenaPackages: areas and CMTCONFIG of the local Athena setup
    group_area: str = ""
    work_area: str = ""
    cmt_config: str = ""


def _resolve_work_env(options, cur_dir, tmp_log):
    """
    Find the run directory and the Athena setup, and set options.cmtConfig

    args:
        options: parsed prun options, with options.workDir already resolved and containing cur_dir
        cur_dir: directory where prun was started
        tmp_log: logger
    returns:
        _WorkEnv. Exits with EC_Config when the Athena setup is missing or doesn't match cur_dir or --cmtConfig
    """
    # imported here as in main: importing Client resolves the panda cache host
    from pandaclient import AthenaUtils

    work_env = _WorkEnv()
    if cur_dir != options.workDir:
        # remove special characters
        w_dir_string = re.sub(r"[\+]", ".", options.workDir)
        work_env.run_dir = re.sub(f"^{w_dir_string}/", "", cur_dir)

    if options.useAthenaPackages:
        # get Athena versions
        has_athena_env, athena_env = AthenaUtils.getAthenaVer()
        if not has_athena_env:
            tmp_log.error("You need to setup Athena runtime to use --useAthenaPackages")
            sys.exit(MiscUtils.EC_Config)
        work_env.work_area = athena_env["workArea"]
        work_env.athena_version = f"Atlas-{athena_env['athenaVer']}"
        work_env.group_area = athena_env["groupArea"]
        work_env.cache_version = athena_env["cacheVer"]
        work_env.nightly_version = athena_env["nightVer"]
        work_env.cmt_config = athena_env["cmtConfig"]
        # the run directory is relative to the work area instead
        s_string = re.sub(r"[\+]", ".", work_env.work_area)
        run_dir = re.sub(f"^{s_string}", "", cur_dir)
        if run_dir == cur_dir:
            error_message = (
                f"You need to run prun in a directory under {work_env.work_area}. "
                f"If {work_env.work_area} is a read-only directory, perhaps you did setup Athena without --testarea or the 'here' tag of asetup."
            )
            tmp_log.error(error_message)
            sys.exit(MiscUtils.EC_Config)
        elif run_dir == "":
            run_dir = "."
        elif run_dir.startswith("/"):
            run_dir = run_dir[1:]
        work_env.run_dir = run_dir + "/"
    elif options.athenaTag != "":
        work_env.athena_version, work_env.cache_version, work_env.nightly_version = AthenaUtils.parse_athena_tag(options.athenaTag, options.verbose, tmp_log)

    # set CMTCONFIG
    options.cmtConfig = AthenaUtils.getCmtConfig(work_env.athena_version, work_env.cache_version, work_env.nightly_version, options.cmtConfig, options.verbose)

    # check CMTCONFIG
    if not AthenaUtils.checkCmtConfig(work_env.cmt_config, options.cmtConfig, options.noBuild):
        sys.exit(MiscUtils.EC_Config)
    return work_env


def main(get_taskparams=False, ext_args=None, dry_mode=False, get_options=False):
    """
    Execute prun command
    :param get_taskparams: get task parameters and return them if True
    :param ext_args: external arguments to be passed to prun
    :param dry_mode: execute prun in dry mode
    :param get_options: get options and return them if True
    :return: task parameters or options if get_taskparams or get_options is True
    """
    # tweak sys.argv
    sys.argv.pop(0)
    sys.argv.insert(0, "prun")

    # parse options and check against the obsolete options first
    optP = PrunArgs.build_parser()
    MiscUtils.remove_obsolete_options(sys.argv, MiscUtils.PRUN_OBSOLETE_OPTIONS)
    options = optP.parse_args(ext_args)

    if options.verbose:
        print(options)
        print()

    # load json
    jsonExecStr, _ = load_json_options(
        options,
        # "exec" is the option name, and inputFileListName the old destination of --inputFileList
        key_aliases={"exec": "jobParams", "inputFileListName": "inputFileList"},
        unknown_key_exit_code=0,
    )

    # return options
    if get_options:
        return vars(options)

    # display version
    if options.version:
        print(f"Version: {PandaToolsPkgInfo.release_version}")
        sys.exit(0)

    # imported after --version: importing Client resolves the panda cache host
    from pandaclient import Client, PLogger, PsubUtils

    # full execution string
    fullExecString = PsubUtils.convSysArgv()
    fullExecString += jsonExecStr

    tmp_log = PLogger.getPandaLogger()

    # use dev server
    if options.devSrv:
        Client.useDevServer()

    # use INTR server
    if options.intrSrv:
        Client.useIntrServer()

    # noCompile uses noBuild stuff
    if options.noCompile:
        options.noBuild = True

    # not skip log files in inDS
    if options.notSkipLog:
        options.useLogAsInput = True

    # old container execution mode
    if options.oldContMode:
        options.alrb = False

    # use runGen
    if options.useAthenaPackages and options.alrb:
        options.directExecInContainer = False

    # container stuff
    if options.containerImage != "":
        options.noBuild = True
        if options.alrb:
            options.useSandbox = True
        if not options.useSandbox:
            tmp_log.warning(
                "Files in the run directory are not sent out by default when --containerImage is used. "
                "Please use --useSandbox if you need those files on the grid."
            )

    # validate --transferType
    invalid = get_invalid_transfer_types(options.transferType)
    if invalid:
        optP.error(f"--transferType: invalid value(s) {', '.join(sorted(invalid))}. Allowed: {', '.join(sorted(VALID_TRANSFER_TYPES))}")

    # files to be deleted
    files_to_delete = []

    # save current dir
    curDir = os.path.realpath(os.getcwd())

    # remove spaces
    options.outputs = options.outputs.replace(" ", "")

    # site, memory, and input options handled the same way as in the other tool
    includedSite = process_common_options(options, tmp_log)

    # set maxNFilesPerJob
    PsubUtils.limit_maxNumInputs = options.maxNFilesPerJob

    # bulk submission
    ioList = get_bulk_io_list(options, tmp_log)

    # enforce to use output dataset container
    if not options.outDS.endswith("/"):
        options.outDS = f"{options.outDS}/"

    # absolute path for PFN list
    if options.pfnList != "":
        options.pfnList = os.path.realpath(options.pfnList)

    # extract DBR from exec
    tmpMatch = re.search("%DB:([^ '\";]+)", options.jobParams)
    if tmpMatch is not None:
        options.dbRelease = tmpMatch.group(1)
        options.notExpandDBR = True

    # check DBRelease, good run list, and event picking
    check_input_options(options, tmp_log)

    # check rootVer
    if options.rootVer != "":
        if options.useAthenaPackages or options.athenaTag:
            tmp_log.warning(
                "--rootVer is ignored when --athenaTag or --useAthenaPackages is used, not to break the runtime environment by superseding the root version"
            )
            options.rootVer = ""
        else:
            # change / to .
            options.rootVer = options.rootVer.replace("/", ".")

    # check writeInputToTxt
    if options.writeInputToTxt != "":
        # remove %
        options.writeInputToTxt = options.writeInputToTxt.replace("%", "")
        # loop over all StreamName:FileName
        for tmpItem in options.writeInputToTxt.split(","):
            tmpItems = tmpItem.split(":")
            if len(tmpItems) != 2:
                tmp_log.error(f"invalid StreamName:FileName in --writeInputToTxt : {tmpItem}")
                sys.exit(MiscUtils.EC_Config)

    # read list of files to be used
    filesToBeUsed = []
    if options.inputFileList != "":
        rFile = open(options.inputFileList)
        for line in rFile:
            line = line.replace("\n", "")
            line = line.strip()
            if line != "":
                filesToBeUsed.append(line)
        rFile.close()

    # remove whitespaces
    if options.inDS != "":
        options.inDS = options.inDS.replace(" ", "")

    # persistent file
    if options.persistentFile:
        options.persistentFile = f"{options.persistentFile}:sources.{MiscUtils.wrappedUuidGen()}.__ow__"

    # warning
    if 0 < options.nFilesPerJob < 5:
        tmp_log.warning(
            "Very small --nFilesPerJob tends to generate so many short jobs which could send your task to exhausted state "
            "after scouts are done, since short jobs are problematic for the grid. Please consider not to use the option."
        )

    if options.maxNFilesPerJob < 5:
        tmp_log.warning(
            "Very small --maxNFilesPerJob tends to generate so many short jobs which could send your task to exhausted state "
            "after scouts are done, since short jobs are problematic for the grid. Please consider not to use the option."
        )

    # warning for nFilesPerJob
    if options.nFilesPerJob and options.nFilesPerJob > options.maxNFilesPerJob:
        tmp_log.warning("--nFilesPerJob cannot be larger than --maxNFilesPerJob. It is set to the value of --maxNFilesPerJob")
        options.nFilesPerJob = options.maxNFilesPerJob

    # check grid-proxy
    if not dry_mode:
        PsubUtils.check_proxy(options.verbose, options.vomsRoles)

    # convert in/outTarBall to full path
    if options.inTarBall != "":
        options.inTarBall = os.path.abspath(os.path.expanduser(options.inTarBall))
    if options.outTarBall != "":
        options.outTarBall = os.path.abspath(os.path.expanduser(options.outTarBall))

    # check working dir
    options.workDir = os.path.realpath(options.workDir)
    if options.workDir != curDir and (not curDir.startswith(f"{options.workDir}/")):
        tmp_log.error(f"you need to run prun in a directory under {options.workDir}")
        sys.exit(MiscUtils.EC_Config)

    # avoid gathering the home dir
    if (
        "HOME" in os.environ
        and not options.useHomeDir
        and not options.useAthenaPackages
        and os.path.realpath(os.path.expanduser(os.environ["HOME"])) == options.workDir
        and not dry_mode
    ):
        tmp_log.warning(
            "prun is executed just under the HOME directory "
            "and is going to send all files under the dir including ~/Mail/* and ~/private/*. "
            "Do you really want that? (Please use --useHomeDir if you want to skip this confirmation)"
        )
        while True:
            tmpAnswer = input("y/N: ")
            tmpAnswer = tmpAnswer.strip()
            if tmpAnswer in ["y", "N"]:
                break
        if tmpAnswer == "N":
            sys.exit(MiscUtils.EC_Config)

    # check maxCpuCount
    if options.maxCpuCount > Client.maxCpuCountLimit:
        tmp_log.error(f"too large maxCpuCount. Must be less than {Client.maxCpuCountLimit}")
        sys.exit(MiscUtils.EC_Config)

    # create tmp dir
    if options.tmpDir == "":
        tmpDir = f"{curDir}/{MiscUtils.wrappedUuidGen()}"
    else:
        tmpDir = f"{os.path.abspath(options.tmpDir)}/{MiscUtils.wrappedUuidGen()}"
    os.makedirs(tmpDir)

    # exit action
    def _onExit(dir, files, del_command):
        for tmpFile in files:
            del_command(f"rm -rf {tmpFile}")
        del_command(f"rm -rf {dir}")

    atexit.register(_onExit, tmpDir, files_to_delete, MiscUtils.commands_get_output)

    # run directory, Athena setup, and CMTCONFIG
    work_env = _resolve_work_env(options, curDir, tmp_log)

    # event picking
    if options.eventPickEvtList != "":
        epLockedBy = "prun"
        if not options.noSubmit:
            # request event picking
            epStat, epOutput = Client.requestEventPicking(
                options.eventPickEvtList,
                options.eventPickDataType,
                options.eventPickStreamName,
                options.eventPickDS,
                options.eventPickAmiTag,
                [],
                options.inputFileList,
                options.outDS,
                epLockedBy,
                fullExecString,
                1,
                options.eventPickWithGUID,
                options.ei_api,
                options.verbose,
            )
            # set input dataset
            options.inDS = epOutput
        else:
            options.inDS = "dummy"
        tmp_log.info(f"requested Event Picking service to stage input as {options.inDS}")

    # additional files
    if options.extFile == "":
        options.extFile = []
    else:
        tmpItems = options.extFile.split(",")
        options.extFile = []
        # convert * to .*
        for tmpItem in tmpItems:
            options.extFile.append(tmpItem.replace("*", ".*"))

    # merge script, glue packages, and excluded files
    process_sandbox_options(options)

    # LFN matching
    if options.match != "":
        # convert . to \.
        options.match = options.match.replace(".", r"\.")
        # convert * to .*
        options.match = options.match.replace("*", ".*")

    # LFN anti-matching
    if options.antiMatch != "":
        # convert . to \.
        options.antiMatch = options.antiMatch.replace(".", r"\.")
        # convert * to .*
        options.antiMatch = options.antiMatch.replace("*", ".*")

    # get job script
    if options.jobParams == "":
        tmp_log.error("you need to give --exec\n  prun [--inDS inputdataset] --outDS outputdataset --exec 'myScript arg1 arg2 ...'")
        sys.exit(MiscUtils.EC_Config)

    # replace : to = for backward compatibility
    for optional_arg in ["RNDM"]:
        options.jobParams = re.sub(f"%{optional_arg}:", f"%{optional_arg}=", options.jobParams)

    # check output dataset
    if options.outDS == "":
        tmp_log.error("no outDS is given\n  prun [--inDS inputdataset] --outDS outputdataset --exec 'myScript arg1 arg2 ...'")
        sys.exit(MiscUtils.EC_Config)

    # check PFN list options
    check_pfn_list_options(options, tmp_log)

    # secondary datasets
    parse_secondary_datasets(options, tmp_log)

    # reusable secondary streams
    if options.reusableSecondary == "":
        options.reusableSecondary = []
    else:
        options.reusableSecondary = options.reusableSecondary.split(",")

    # get nickname
    if not dry_mode:
        nickName = PsubUtils.getNickname()
    else:
        nickName = "dummy"

    if nickName == "":
        sys.exit(MiscUtils.EC_Config)

    # set Rucio accounting
    PsubUtils.setRucioAccount(nickName, "prun", True)

    # check split options
    check_split_options(options, tmp_log)

    # check scouting options
    check_scouting_options(options, tmp_log)

    # translate --nJobs into nFiles / nFilesPerJob
    set_n_files_from_n_jobs(options)

    #####################################################################
    # archive sources and send it to HTTP-reachable location
    archiveName = None
    if (options.containerImage == "" or options.useSandbox) and not dry_mode:
        if options.inTarBall == "" and options.tarBallViaDDM == "":
            archiveName = create_sandbox_archive(options, curDir, tmpDir, work_env.run_dir, work_env.work_area, work_env.group_area, files_to_delete, tmp_log)
        else:
            archiveName, _ = PsubUtils.use_existing_sandbox(options, tmpDir)

        # save for --outTarBall, and upload the sandbox and the good run list
        archiveName = PsubUtils.upload_sandbox(options, archiveName, curDir, tmp_log, files_to_delete, use_cache_srv=options.vo is None)

    #####################################################################
    # make task
    task_param_map = _build_task_params(
        options,
        work_env=work_env,
        archive_name=archiveName,
        included_site=includedSite,
        full_exec_string=fullExecString,
        files_to_be_used=filesToBeUsed,
        tmp_log=tmp_log,
    )

    #####################################################################
    # submission

    exitCode = 0
    dumpList = []

    # submit task
    for iSubmission, ioItem in enumerate(ioList):
        if options.verbose:
            PsubUtils.print_submission_parameters(
                options.site,
                work_env.athena_version,
                work_env.group_area,
                work_env.cache_version,
                work_env.nightly_version,
                work_env.run_dir,
                [("exec", options.jobParams)],
            )

        if len(ioList) == 1:
            new_task_param_map = task_param_map
        else:
            # replace input and output
            options.inDS = ioItem["inDS"]
            options.outDS = ioItem["outDS"]
            new_task_param_map = PsubUtils.replaceInputOutput(task_param_map, ioItem["inDS"], ioItem["outDS"], iSubmission)
        exit_code = 0
        tmp_output = ""
        taskID = None
        # check outDS format
        if not dry_mode and not PsubUtils.checkOutDsName(
            options.outDS,
            options.official,
            nickName,
            options.mergeOutput,
            options.verbose,
        ):
            tmp_output = f"invalid output datasetname:{options.outDS}"
            tmp_log.error(tmp_output)
            exit_code = MiscUtils.EC_Config
        # check task parameters
        if exit_code == 0:
            exit_code, tmp_output = PsubUtils.checkTaskParam(new_task_param_map, options.unlimitNumOutputs)
        if exit_code != 0 and len(ioList) == 1:
            sys.exit(exit_code)
        if options.noSubmit and options.verbose:
            PsubUtils.print_task_params(new_task_param_map, tmp_log)
        if options.dumpTaskParams is not None:
            with open(os.path.expanduser(options.dumpTaskParams), "w") as f:
                json.dump(new_task_param_map, f)
        if get_taskparams:
            os.chdir(curDir)
            try:
                shutil.rmtree(tmpDir)
            except Exception:
                pass
            return new_task_param_map
        if not options.noSubmit and exit_code == 0:
            exit_code, tmp_output, taskID = PsubUtils.submit_task(new_task_param_map, options, tmp_log)
        dumpList.append(PsubUtils.make_dump_item(options, exit_code, tmp_output, taskID, iSubmission, len(ioList)))

    # go back to current dir
    os.chdir(curDir)
    # dump
    if options.dumpJson is not None:
        with open(os.path.expanduser(options.dumpJson), "w") as f:
            json.dump(dumpList, f)
    # succeeded
    sys.exit(exit_code)
