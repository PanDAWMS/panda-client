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
          import json, sys, shlex, os, traceback, ast, base64, glob, shutil
          from pandaclient import PLogger
          log_stream = PLogger.getPandaLogger(False)
          from pandaclient import PhpoScript
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
          # use <br> for \n since \n is sometimes converted to n when python is executed through cwl-runner
          msg_str = '<br>'
          msg_str += '     type: phpo<br>'
          argStr = ' '.join(shlex.quote(x.strip()) for x in args)
          msg_str += '     args: {}<br>'.format(argStr)
          msg_str += ' training: {}<br>'.format(', '.join(newInDsList))
          task_params = PhpoScript.main(True, args, True)
          for item in task_params['jobParameters']:
              if item["type"] == "template" and item["param_type"] == "output":
                  outputs.append(item["dataset"])
          str_outputs = ','.join(outputs)
          x = {"outDS": str_outputs}
          msg_str += '   output: {}<br>'.format(str_outputs)
          log_stream.info('<base64>:' + base64.b64encode(msg_str.encode()).decode())
          print(json.dumps(x))
      except Exception as e:
          log_stream.error(str(e) + traceback.format_exc())
          sys.exit(1)
