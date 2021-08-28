cwlVersion: v1.0
class: CommandLineTool
baseCommand: python
stdout: cwl.output.json

inputs:
  opt_exec: string
  opt_args: string
  opt_inDS:
    type:
      - "null"
      - string
      - string[]
  opt_inDsType:
    type:
      - "null"
      - string
      - string[]
  opt_secondaryDSs:
    type:
      - "null"
      - string
      - string[]
  opt_secondaryDsTypes:
    type:
      - "null"
      - string[]
outputs:
  outDS:
    type:
      - string

arguments:
  - prefix: '-c'
    valueFrom: |
      try:
          import json, sys, shlex, os, traceback, ast, base64
          from pandaclient import PLogger
          log_stream = PLogger.getPandaLogger(False)
          from pandaclient import PrunScript
          outDS = os.environ['WORKFLOW_OUTPUT_BASE'] + '_<suffix>'
          args = r"""$(inputs.opt_args)"""
          args = args.split()
          args += ['--exec', r"""$(inputs.opt_exec)"""]
          args += ['--outDS', outDS]
          outputs = []
          inDS = r"""$(inputs.opt_inDS)"""
          try:
              inDS = ast.literal_eval(inDS)
          except Exception:
              inDS = [inDS]
          inDsType = r"""$(inputs.opt_inDsType)"""
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
          secondaryDSs = r"""$(inputs.opt_secondaryDSs)"""
          if secondaryDSs == 'null':
              newSecDsList = None
          else:
              try:
                   secondaryDSs = ast.literal_eval(secondaryDSs)
              except Exception:
                   secondaryDSs = [secondaryDSs]
              secondaryDsTypes = r"""$(inputs.opt_secondaryDsTypes)"""
              if secondaryDsTypes == 'null':
                  secondaryDsTypes = [None] * len(secondaryDSs)
              else:
                  try:
                      secondaryDsTypes = ast.literal_eval(secondaryDsTypes)
                  except Exception:
                      secondaryDsTypes = [secondaryDsTypes]
              newSecDsList = []
              for secDsStr, secDsType in zip(secondaryDSs, secondaryDsTypes):
                  inputs = secDsStr.split(',')
                  if len(inputs) > 1:
                      newSecDS = 'UNRESOLVED'
                      for input in inputs:
                          if secDsType and secDsType in input:
                              newSecDS = input
                              break
                  else:
                      newSecDS = secDsStr
                  newSecDsList.append(newSecDS)
          # use <br> for \n since \n is sometimes converted to n when python is executed through cwl-runner
          msg_str = '<br>'
          msg_str += '     type: prun<br>'
          argStr = ' '.join(shlex.quote(x.strip()) for x in args)
          secDsIdx = 1
          if newSecDsList:
              for secDsStr in newSecDsList:
                  argStr = argStr.replace('%%{}%%'.format(secDsIdx), secDsStr)
          msg_str += '     args: {}<br>'.format(argStr)
          msg_str += '    input: {}<br>'.format(', '.join(newInDsList))
          if newSecDsList:
              msg_str += '         : {}<br>'.format(newSecDsList)
          task_params = PrunScript.main(True, args, True)
          for item in task_params['jobParameters']:
              if item["type"] == "template" and item["param_type"] == "output":
                  outputs.append(item["container"])
          str_outputs = ','.join(outputs)
          x = {"outDS": str_outputs}
          msg_str += '   output: {}<br>'.format(str_outputs)
          log_stream.info('<base64>:' + base64.b64encode(msg_str.encode()).decode())
          print(json.dumps(x))
      except Exception as e:
          log_stream.error(str(e) + traceback.format_exec())
          sys.exit(1)
