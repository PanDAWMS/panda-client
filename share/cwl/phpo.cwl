cwlVersion: v1.0
class: CommandLineTool
baseCommand: python
stdout: cwl.output.json

inputs:
  opt_args: string
  opt_trainingDS:
    type:
      - "null"
      - string
      - string[]
  opt_trainingDsType:
    type:
      - "null"
      - string
      - string[]

outputs:
  outDS:
    type:
      - string

arguments:
  - prefix: '-c'
    valueFrom: |
      try:
          import json, sys, os, traceback, ast, base64, glob, shutil
          try:
              from pipes import quote
          except Exception:
              from shlex import quote
          from pandaclient import PLogger
          log_stream = PLogger.getPandaLogger(False)
          from pandaclient import PhpoScript
          from pandaclient.pflow_checker import make_message, encode_message, emphasize_single_message
          outDS = os.environ['WORKFLOW_OUTPUT_BASE'] + '_<suffix>'
          srcDir = os.environ['WORKFLOW_HOME']
          for srcFile in glob.glob(os.path.join(srcDir, '*.json')):
              shutil.copy(srcFile, os.getcwd())
          args = r"""$(inputs.opt_args)"""
          args = args.split()
          args += ['--outDS', outDS]
          outputs = []
          inDS = r"""$(inputs.opt_trainingDS)"""
          try:
              inDS = ast.literal_eval(inDS)
          except Exception:
              inDS = [inDS]
          inDsType = r"""$(inputs.opt_trainingDsType)"""
          if inDsType == 'null':
              inDsType = None
              inDsType = [None] * len(inDS)
          else:
              inDsType = inDsType.split(',')
          newInDsList = []
          for tmpDsStr, tmpDsType in zip(inDS, inDsType):
              inputs = tmpDsStr.split(',')
              if len(inputs) > 1:
                  newInDS = 'UNRESOLVED'
                  for input in inputs:
                      if tmpDsType and tmpDsType in input:
                          newInDS = input
                          break
              else:
                  newInDS = tmpDsStr
              newInDsList.append(newInDS)
          # dump
          msg_str = ''
          msg_str = make_message('     type: phpo', msg_str)
          argStr = ' '.join(quote(x.strip()) for x in args)
          msg_str = make_message('     args: {}'.format(argStr), msg_str)
          msg_str = make_message(' training: {}'.format(', '.join(newInDsList)), msg_str)
          task_params = PhpoScript.main(True, args, True)
          for item in task_params['jobParameters']:
              if item["type"] == "template" and item["param_type"] == "output":
                  outputs.append(item["dataset"])
          str_outputs = ','.join(outputs)
          x = {"outDS": str_outputs}
          msg_str = make_message('   output: {}'.format(str_outputs), msg_str)
          log_stream.info(encode_message(msg_str))
          # check
          invalid = False
          if newInDsList and 'UNRESOLVED' in newInDsList:
              log_stream.error(emphasize_single_message('Traning data was UNRESOLVED. '
                                                        'Check opt_trainingDS and/or opt_trainingDsType'))
              invalid = True
          print(json.dumps(x))
          if invalid:
              sys.exit(2)
      except Exception as e:
          log_stream.error(str(e) + traceback.format_exc())
          sys.exit(1)
