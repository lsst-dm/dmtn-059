Description
===========

The goal was to see if can run a pipeline similar to HSC pipeline
through an existing production workflow system.

1. Pipeline steps: ``processCcd``, ``makeCoaddTempExp``, ``assembleCoadd``, ``detectCoaddSources``, ``mergeCoaddDetections``, ``measureCoaddSources``, ``mergeCoaddMeasurements``, ``forcedPhotCoadd``
2. Use the command-line task version of each pipeline step
3. LSST software stack: ``setup obs_subaru -t w_2017_14``
4. Data inputs: ``ci_hsc`` data configured to use different skymap configuration containing 4 patches within 1 tract (to match testing by Hsin-Fang: `DM-8603 <https://jira.lsstcorp.org/browse/DM-8603>`_)
5. Chose to use DESDM's framework for the existing production workflow system.
6. Only use the butler inside a compute job. If multiple steps were done inside same compute job, they shared the same butler repository.
7. See whether any code changes are necessary to the DESDM framework.

Note: This exercise was done with minimal help outside NCSA.

Questions/problems may already have solutions in software or future designs.

Summary
=======

- Successfully ran ``processCcd`` through ``forcedPhotCoadd`` via command line tasks
- Successfully ran through the DESDM framework having all the features that comes with using DESDM.
- Worth looking into larger scale running (e.g., running of full HSC data set).
- Coding required:

  -  0 DESDM framework changes
  -  Minor LSST stack changes (see Patch Naming Problem)
  -  LSST specific plugins (metadata gathering, specialized query codes, wrapper)

Example Output
==============

(Including a few queries that illustrate only a tiny amount of operational and for-release information stored by DESDM)

Find deepcoadd calexp files (tagged final run with PROTO-0.1)::

    > select f.path ||'/'||f.filename from file_archive_info f, desfile d, proctag t where d.filetype='deepcoadd_calexp' and t.tag='PROTO-0.1' and t.pfw_attempt_id=d.pfw_attempt_id and f.desfile_id=d.id order by f.filename;

    F.PATH\|\|'/'\|\|F.FILENAME
    -------------------------------------------------------------------------

    MMG/hsctest/MDEV/r11/Ht0/p95/coadd/calexp-HSC-I-0-8x6-r11p95.fits
    MMG/hsctest/MDEV/r11/Ht0/p95/coadd/calexp-HSC-I-0-8x7-r11p95.fits
    MMG/hsctest/MDEV/r11/Ht0/p95/coadd/calexp-HSC-I-0-9x6-r11p95.fits
    MMG/hsctest/MDEV/r11/Ht0/p95/coadd/calexp-HSC-I-0-9x7-r11p95.fits
    MMG/hsctest/MDEV/r11/Ht0/p95/coadd/calexp-HSC-R-0-8x6-r11p95.fits
    MMG/hsctest/MDEV/r11/Ht0/p95/coadd/calexp-HSC-R-0-8x7-r11p95.fits
    MMG/hsctest/MDEV/r11/Ht0/p95/coadd/calexp-HSC-R-0-9x6-r11p95.fits
    MMG/hsctest/MDEV/r11/Ht0/p95/coadd/calexp-HSC-R-0-9x7-r11p95.fits

    8 rows selected.


.. figure:: /_static/calexp-HSC-I-0-8x7-r11p95.jpeg
    :name: calexp-HSC-I-0-8x7-r11p95.jpeg (created jpeg using DS9 on fits file)


Query DB to see how many executions of each step::

    > select modname, count(*) from pfw_wrapper w, pfw_attempt a where w.pfw_attempt_id=a.id and a.reqnum=11 and a.attnum=95 group by modname order by min(w.wrapnum);

    MODNAME COUNT(\*)
    --------------------------------------------------- ----------
    create_butler_registry 1
    processccd 16
    make_coadd_temp_exp 32
    assemble_coadd 8
    detect_coadd_sources 8
    merge_coadd_detections 4
    measure_coadd_sources 8
    merge_coadd_measurements 4
    forced_phot_coadd 8

    9 rows selected.

Query DB for how long wallclock time it took to run (including all setup, db read/writes and file transfers)::

    > select end_time - start_time from task t, pfw_attempt a where t.id=a.task_id and a.reqnum=11 and a.attnum=95;

    END_TIME-START_TIME
    ---------------------------------------------------------------------------
    +000000000 00:20:20.628962

Concept matching between DESDM FW and LSST
==========================================

-  Not including comparison on release process and delivery of data to end users.

-  Custodial Store: 

   -  DESDM: (“Home Archive”) File system, backups to spinning archive and tape at Fermilab.

   -  LSST: Part of the Data Backbone (still in design phase),

-  (Global) File Registry:

   -  DESDM: Central Oracle DB (separate for dev/tests via framework, production, release). Tracks physical information (file size, md5sum) of any output file produced and relative paths of every file in custodial store. During compute job, physical file attributes are immediately stored in file registry regardless of whether file is kept.

   -  LSST: Part of the Data Backbone (still in design phase - current plans are to make similar to DESDM with a consolidated DB).

-  (Global) Metadata:

   -  DESDM: Central Oracle DB (separate for dev/tests via framework, production, release). “Interesting” science metadata used to find files (e.g., band, ccdnum, expnum, image corners, etc).  Filetypes are used to determine what metadata is stored for each file.

-  Execution Provenance:

   -  DESDM: Central Oracle DB (separate for dev/tests via framework, production, release). Job info: Software stack, job start/end; per execution info: start/end, exec host, usage 

   -  LSST:

      -  Current:
      -  Future: ``ctrl_stats`` (condor job information), job env, (others?  Still in design phase)

-  (Global) File Provenance:

   -  DESDM: Central Oracle DB (separate for dev/tests via framework, production, release). Open Provenance Model: used (execution + file), was generated by (execution + file), was derived from (file + file)

   -  LSST:

      -  Current:
      -  Future: design phase, current non-operation plans seem to revolve around tracking butler repository instead of file based.

-  Determining inputs:

   -  DESDM: allows for filename patterns or metadata queries to find input filenames + metadata. There is generic query language and the ability to plug in specialized query code.

   -  LSST:

      -  Current: data ids on command line (manual via script) + searching Butler repo
      -  Future: dataIdGenerator? (still in design/prototyping phase)

-  Dividing inputs into sets for single execution:

   -  DESDM: Has wrapperloop, divide\_by and match on metadata keys (like visit, ccd). Haven’t needed it yet, but if had really complicated “formula”, a specialized query could be written that labels each file with a grouping name and tell the framework to divide based upon the group name.

   -  LSST:

      -  Current: ``ctrl_pool`` + butler
      -  Future: supertask’s ``define_quanta`` (still in design/prototyping phase)

-  Telling executable about its input files:

   -  DESDM: Input files are either listed on command line or appear in a list file specified on the command line. In both cases, the path to the file is included. Has the ability to include other (metadata) per line (e.g., expnum ccd band magzero)

   -  LSST:

      -  Current: Data ids are listed on the command line (e.g., ``--selectId visit=12345 ccd 32``). The Butler takes data ids, uses the policy templates to find the file.

      -  Future: If using command line task will be the same (future butler may not be using templates, but an internal file catalog to find files). Supertask: one gives the supertask a quantum which includes dataset type and data ids (still in design/prototyping phase)

-  Naming outputs:

   -  DESDM: Operator defines patterns which are expanded to be put on command lines. Operator controls filename uniqueness using framework submission ids 

      -  request number (can be as large as a campaign, but has been used to break campaign into smaller chunks for operational reasons)
      -  Unitname (e.g., expnum or tilename)
      -  processing attempt number (how many times same submission was tried)

   -  LSST:

      -  Current: Butler uses policy files containing path + filename templates. Templates can can be overridden.
      -  Future: 

-  Staging files from tape for use by production:

   -  DESDM: Doesn’t need this. All files are stored on disk (only backups on tape)

   -  LSST:

      -  Current: All files currently on disk
      -  Future: Campaign manager(?) would call code to stage files from tape to disk.

-  Transferring files to/from disk visible by compute job:

   -  DESDM: From inside job, uses http (or local file copy for local test runs) to copy file from home archive to non-shared disk visible by compute job. Has capability within pipeline submit to pre/post stage files to/from shared disk on compute cluster using globus. Has not been used in production nor tested in long time. DESDM tracks files on shared disk in same file catalog as home archive (called a target archive)

   -  LSST:

      -  Current: Assumes files are in Butler repository on shared disk visible by all compute machines.
      -  Future: Pegasus would stage files from disk local to compute site (e.g., shared file system) to job work space.

-  File locations inside compute job:

   -  DESDM: Operator defines directory patterns explicitly for inside job. Because some of the executables put full filenames inside comments, folks requested the operators to make these paths more shallow than those that would be inside the home archive (i.e., there is a requirement that compute job relative paths can be different than home archive paths)

   -  LSST:

      -  Current: Butler uses policy files containing path + filename patterns. Policy files can be overridden
      -  Future: Same as current?

-  Executing a pipeline step:

   -  DESDM: Wrappers are the interface between the hostile code and the framework. There is a generic wrapper which works for the majority of DES use cases.

   -  LSST

      -  Current: Command-line task.
      -  Future: SuperTask mostly still in design/prototyping phase (SuperTask WG)

-  Executing a series of pipeline steps within a single compute job:

   -  DESDM: Operator tells at submission time what steps to do in a single compute job.

   -  LSST

      -  Current: ``ctrl_pool`` or own scripting,
      -  Future: Composite SuperTask (design/prototyping phase)

-  Executing a set of pipeline steps in parallel within a single compute job:

   -  DESDM: Framework allows operator to say run up to X of the same step at the same time. Uses python multiprocessing. Also does framework work for each step in parallel (e.g., copying input files, saving output file metadata, etc)

-  Executing a series of pipeline steps within a single compute job passing file in memory:

   -  DESDM: Framework cannot do this and steps are separate executables. If had steps as python codes, could write a specialized wrapper to run the steps in sequence passing file in memory. If want a more generic framework for this, the specialized wrapper would have to grow to a generic framework.

   -  LSST:

      -  Current:
      -  Future: In design/prototyping phase. Requires changes to Butler and needs composite Supertask.

-  Control of multiple compute jobs to do independent pipeline steps:

   -  DESDM: HTCondor (DAGMan)

   -  LSST:

      -  Current: ``ctrl_pool`` using batch processing system like Slurm
      -  Future: Pegasus (prototyping phase)

-  Configuration/Submission of multiple pipelines:

   -  DESDM: Operator built tools: configuration version controls (Pipebox in svn), mass submission, automatic submission (nightly), automatic failure resubmission (SNe, others?) 

   -  LSST:

      -  Current: ``ctrl_pool``
      -  Future: Campaign Manager

-  Monitoring submissions:

   -  DESDM: desstat (thin “science” layer around condor\_q), print\_job.py (shows status inside a compute job by querying central DB), summary web pages, loads of information within DB that can be queried, summarized, etc.

   -  LSST:

      -  Current: whatever batch system status (e.g., qstat)
      -  Future: Campaign Manager should have views.

-  Monitoring pipeline status within a compute job especially if multiple steps:

   -  DESDM: Updates database at every state change (transferring input files, starting this step, finished this step, saving output provenance and metadata, etc)

   -  LSST:

      -  Current: ``ctrl_pool`` cannot do this (job is blackbox). Could go looking for log files.
      -  Future: At one time had event monitor (watching log messages), but that’s been set aside. Current plans seem to be run only 1 step per job and then have the job management monitoring software.

-  Querying messages in stdout/stderr/logs:

   -  DESDM: QCFramework. Operator defines patterns to match in stdout/stderr. QCF can put into DB immediately. Joinable to other tables (framework statistics, files, etc).

   -  LSST:

      -  Current: Can manually look for log files on compute machine.
      -  Future: At one time had event monitor (watching log messages), but that’s been set aside. Current plans are bring log files home at end of job and slurp them into something like logstash (how does one join to other DB tables?)

Work done to provide prototype 0.1
==================================

1.  DESDM and LSST metadata terminology different enough that was easier to get started by making LSST specific metadata tables (e.g., ccd instead of ccdnum)
2.  0 changes needed to be made to the DESDM framework itself. Some plugins and specialized wrappers and query codes needed to be written.
3.  Ingest HSC raw files into file catalog, metadata tables

    a. DESDM allows plugins for file ingestion. Wrote an HSC raw plugin.  Since HSC raw files are fits file should be close to normal DES file ingestion.

       i.  Was going to use pre-defined LSST functions to convert headers to values (e.g., expid or frameid into visit), but those require special LSST metadata object (as opposed to taking one header value and converting it) So, for now I copied sections of the LSST code into functions that take header value(s) and converts them.

       ii. Only saves enough metadata to run test pipeline plus any other values DESDM stored (e.g., airmass) that could be read directly from the headers (e.g., didn’t save metadata also in a visit table in addition to image table or save image corners).

4.  For every new DESDM filetype needed to add definitions describing on how to gather metadata.

    a. As mentioned in the raw section, currently treating files as regular fits files and using the same mechanism to read the files as DESDM. With the afwimage layer and butler layer trying to abstract away the format of the file, this is probably not the long term solution.

5.  Manually ran command-line tasks to produce schema files. Saved with unique filenames (e.g., :file:`deepCoadd_peak_Vw_2017_14.fits` where `w_2017_14` is the software stack version) in DESDM archive.  (Wrote a script to make it easier to generate new files. But could write a pipeline to do this which would automatically put files in home archive.)

    a. A later conversation with Jim Bosch on a SuperTask call indicated that I didn’t really know what these schema files where. I assumed they were how the code was told to build the catalog files (similar to the astr0matic param files). But Jim said that is not the case. That the science code checks the given schema file to see if it matches what it expects to do and if mismatch aborts. So longer term we need to understand if we need to make these schema files at all and how one changes what values are put into the catalog, i.e., is it always a code change or is there a configuration change.

6.  DESDM framework has a wrapper class that acts as the interface layer between the “hostile” executable and the framework. Needed to write an LSST specific wrapper.

    a. First wrapper inside a job sets up the butler repository for the job. It takes a file containing butler policy templates and replaces DESDM keyword variables in them to make unique filenames (e.g., reqnum and attnum).

       i.  This works where all the input files come from the same run.  Need to talk to Nate to figure out how to tell butler more than 1 pattern for the same datasetType (or butler changes to be a mini DBB with metadata -> relpath filename mappings)

       ii. The Butler also requires sqlite3 files. See Butler concerns section for more details. The wrapper either has to call codes to create the sqlite3 files or since the contents are not really a file registry these files could be pre-created for a set of runs using same set of calibration files. This first attempt tried calling codes to create exactly the sqlite3 file that matched the files in the job repository.

    b. Command line tasks do not take lists of inputs. Instead the dataIds are put on the command line (e.g., ``--selectId visit=903344 ccd=11 --selectId visit=903344 ccd=5 --selectId visit=903336 ccd=24 ...``).
       Created submit WCL syntax to tell the wrapper to add that information to the command line for every input file of a particular type (``per_file_cmdline = list.corr.img_corr:--selectId visit=$(visit) ccd=$(ccd)``)

    c. The reference catalog consists of many files. Currently the pipeline assumes all of the reference catalog is in place and looks up what file it actually needs. As far as I know these files are not accessed via the Butler. I tarred up the test set of files (:file:`189584.fits`, :file:`189648.fits`, :file:`config.py`, :file:`master_schema.fits`, :file:`README.txt`) and tracked the tarball as any other input file (this also gets around needing unique filenames if we ever have more than 1 version of these ref cats). The operator can tell the framework to untar the tarball and this new wrapper performs that task.

       i. Need to look into how to handle this in the future especially when a tarball of the full reference catalog could be really large.

7.  Ran ``processCcd.py`` through framework on single explicit visit+ccd.

8.  Run ``processCcd.py`` on visit+ccd for tract.

    a. Need capability to find visit+ccd ids for tract. Coded a workaround for a true spatial query. Created a file with tract, patch, visit, ccd rows. A new specialized query code reads the file, gets the visit ccd values and queries the DB to find the actual raw images.

    b. Future work would take the sqlite3 table Hsin-Fang created that has all the mappings for the HSC data, ingest it into the Oracle database, and modify query to use it instead of the text file.

    c. DES processing would normally do a science query that uses image corners to find overlaps. Not sure whether LSST would do the overlap queries live or pre-create the overlap table as in previous note.

9.  ``makeCoaddTempExp`` through framework

    a. Needed to handle patch name containing comma (see Patch naming problem)

10. The merge steps (``merge_coadd_detections`` and ``merge_coadd_measurements``) needed new command line syntax: ``filter=HSC-I^HSC-R`` (again used by the butler to find files which we already have a list and are having to reverse engineer into dataId command lines). Added new submit wcl syntax and code to the new wrapper to use it. Example: ``add_cmdline = '^'.join(list.det.deepcoadd_det.filter)``

Patch Naming Problem
====================

Current LSST science pipelines use a comma to separate 2 coordinates for a patch (e.g., ``8,6``).

This puts a comma in the filename when the filename contains the patch.  The comma also causes problems with DES framework which treats the comma as an "and". In many points during the work initialization, the DES framework expands the patch to be 2 separate patches (e.g. patch 8 and patch 6).

Workaround: In my copy of stack, changed the comma to be ``'x'``. Required changes in:

-  ``obs_subaru/13.0-18-g552d3b8/python/lsst/obs/hsc/hscMapper.py``

   .. code-block:: python

      patchX, patchY = [int(patch) for patch in dataId['patch'].split(',')]

-  ``meas_base/13.0-6-gac12f96/python/lsst/meas/base/forcedPhotCoadd.py``

   .. code-block:: python

      patch = tuple(int(v) for v in dataRef.dataId["patch"].split(","))

-  ``meas_base/13.0-6-gac12f96/python/lsst/meas/base/references.py``

   .. code-block:: python

      dataId = {'tract': tract, 'patch': "%d,%d" % patch.getIndex()}

-  ``pipe_tasks/13.0-18-gb0831f2/python/lsst/pipe/tasks/coaddBase.py``

   .. code-block:: python

      patchIndex = tuple(int(i) for i in patchRef.dataId["patch"].split(","))

-  Not needed for HSC testing: ``obs_subaru/13.0-18-g552d3b8/python/lsst/obs/suprimecam/suprimecamMapper.py``

   .. code-block:: python

      patchX, patchY = [int(patch) for patch in dataId['patch'].split(',')]

Butler questions/concerns
=========================

-  Currently only using Butler for inside of jobs (because science pipelines requires it). If multiple steps were done inside same compute job, they shared the same butler repository.
-  Registry.sqlite3, (non-Butler) ingestImages.py, calibRegistry.sqlite3, (non-Butler) ingestCalibs.py.  Took a while to understand that the Butler registry files aren’t really a file registry. I kept trying to run ingestImages.py on non-raw files (e.g., image output of processCCD) to initialize a butler from scratch. The Butler registry for images is more of a list of data id combinations (visit + ccd) to be used in cases where not enough data ids are included to find the file.
-  Running ingestImages.py or ingestCalibs.py must either use directory structure/filenames to determine information to put in the .sqlite3 file. Doing this once per job is too costly. Doing it once per large subset of campaigns wouldn’t be as expensive.
-  Heard mentions of Butler’s sqlite3 file growing into a mini DBB where metadata can be mapped to rel path + filename (i.e., a real file registry). In most ways this will more fit normal operations. The downside would be creating the initial registry per job. We’d want to limit the number of times files have to be opened to read metadata (which the production framework could have already retrieved from the global metadata service).
   -  Need to follow through with Nate and K-T.
-  Must rename/make soft link to HSC raw files because filename does not contain enough metadata (would be fixed with a real file registry)
-  Future work is needed to keeping operations filename and directory patterns for inside the job in sync with Butler filename and directory patterns
-  Need to request a function in Butler to dump merged policy definitions so that we have an easy place to manually make changes (i.e., know exactly all the datasetTypes it controls) as well as help debug file naming issues.
-  As mentioned in the work section, current Butler policy workaround will not work in operations if input files of same datasetType come from different processing attempts (i.e., if different reqnum, attnum). So need to discuss with Nate what real Butler solution is.


Example submit wcl
=========================

The following is example submit wcl that contains the operations instructions for
executing the makeCoaddTempExp.

.. While not xml (wcl actually based upon apache config, but apacheconf isn't currently recognized as a valid code-block language - perhaps not using latest version)
.. code-block:: xml

    <make_coadd_temp_exp>
        <exec_1>  # label telling wrapper that this is the 1st exec to run
            # example command line:   
            #    makeCoaddTempExp.py jobrepo --output jobrepo --id tract=0 patch=8x7 filter=HSC-I --doraise 
            #            -c doApplyUberCal=False --selectId visit=904010 ccd=10 --selectId visit=904010 ccd=4

            execname = makeCoaddTempExp.py   # what executable to run (must be in path inside compute job)

            cmd_hyphen = mixed_gnu  # use single hyphen for single char options and 
                                    # double hyphen for multiple char options
            <cmdline>
                _01 = ${job_repo_dir}   # positional argument
                output = ${job_repo_dir}
                id = tract=${tract} patch=${patch} filter=${filter}
                doraise = _flag    # option is a flag
                c = doApplyUberCal=False
            </cmdline>

            # open provenance model (minus was_derived_from)
            used = list.corr.img_corr, file.skymap, file.butler_registry, file.butler_template
            was_generated_by = file.deepcoadd_tempexp
        </exec_1>
        <file>
            <butler_registry>    # generated by first step in pipeline
                # how to name input file
                filepat = generic
                flabel = registry
                fsuffix = sqlite3

                # where to put it (jobroot=job_enddir, archive=ops_enddir)
                dirpat = generic_norepo
                rename_file = registry.sqlite3     # Butler requires it to be this filename (note: not unique)
                job_enddir = ${job_repo_dir}
            </butler_registry>
            <butler_template>    # File containing patterns to create unique filenames.   Used to create Butler config
                # how to name input file
                filename = butler_templates-${config_version}.wcl

                # where to put it 
                dirpat = generic_norepo
                job_enddir = config
            </butler_template>
            <skymap>
                filename = skyMap-${skymap_version}.pickle
                rename_file = skyMap.pickle   # science code requires this filename (note: not unique)
                dirpat = generic_repo
                job_enddir = deepCoadd
            </skymap>
            <img_corr>
                listonly = True   # what files already determined in list section.
                                  # this tells framework where to put the files (as opposed to the list itself)

                # where to put it (jobroot=job_enddir, archive=ops_enddir)
                dirpat = hsc_ccd
                ops_enddir = img
                job_enddir = corr
            </img_corr>

            ### output files
            <deepcoadd_tempexp>
                # what metadata to save
                filetype = deepcoadd_tempexp

                # how to name output file
                filepat = hsc_tract_patch_visit_filter
                flabel = warp
                fsuffix = fits

                # whether to save or compress
                savefiles = true
                compress_files = false

                # where to put it (jobroot=rundir, archive=ops_enddir)
                # inside jobroot must match Butler template definition for this type (so sync problem)
                dirpat = hsc_tract_patch_filter
                ops_enddir = coadd
                job_outtype = deepCoadd
            </deepcoadd_tempexp>
        </file>
        <list>
            <corr>
                # how to get list data
                exec = hsc_dummy_query_corr.py
                args = --section ${submit_des_db_section} --tractinfo ${tractinfo} --tract ${tract} \ 
                       --qoutfile ${qoutfile} --pfw_attempt_id ${query_pfw_attempt_id}

                # how to create lists
                divide_by = tract, patch, visit, filter      # define_quanta
                columns = img_corr.fullname, tract, patch, visit, ccd, filter

                # what to name the list file
                filepat = list_tract_patch_visit_filter 
                flabel = ${modulename}_corr
                fsuffix = list

                # where to put it (jobroot=rundir, archive=ops_enddir)
                dirpat = generic_norepo
                ops_enddir = list/${modulename}
                rundir = list/${modulename}
            </corr>
        </list>
        <wrapper>   # wrapper specific values
            # new: wrapper adds one of these for each file in list.corr.img_corr
            per_file_cmdline = list.corr.img_corr:--selectId visit=$(visit) ccd=$(ccd)    

            job_repo_dir = ${job_repo_dir}
            mapper = lsst.obs.hsc.HscMapper    # which butler mapper to use, needed to set up Butler
            butler_template = ${file.butler_template.fullname} # needed to set up Butler, per_file_cmdline, etc
        </wrapper>
        wrappername = genwrap_lsst.py  # needed a lsst specific wrapper in order to set up Butler
        wrapperloop = tract,patch,visit,filter   # define_quanta (how many times to we run this)
        loopobj = list.corr   # what data is used along with wrapperloop
        modnamepat = ${modnamepat_tract_patch_visit_filter}  # how to name internal files like wrapper wcl, log files
    </make_coadd_temp_exp>


