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
          import json, sys, shlex, os, traceback, ast, base64, re
          from pandaclient import PLogger
          log_stream = PLogger.getPandaLogger(False)
          from pandaclient import PrunScript
          from pandaclient.pflow_checker import make_message, encode_message, emphasize_single_message
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
          # dump
          msg_str = ''
          msg_str = make_message('     type: prun', msg_str)
          argStr = ' '.join(shlex.quote(x.strip()) for x in args)
          if newSecDsList:
              secDsIdx = 1
              for secDsStr in newSecDsList:
                  argStr = argStr.replace('%%DS{}%%'.format(secDsIdx), secDsStr)
                  secDsIdx += 1
          msg_str = make_message('     args: {}'.format(argStr), msg_str)
          msg_str = make_message('    input: {}'.format(', '.join(newInDsList)), msg_str)
          if newSecDsList:
              msg_str = make_message('         : {}<br>'.format(newSecDsList), msg_str)
          task_params = PrunScript.main(True, args, True)
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
              log_stream.error(emphasize_single_message('Input was UNRESOLVED. Check opt_inDS and/or opt_inDsType'))
              invalid = True
          if newSecDsList and 'UNRESOLVED' in newSecDsList:
              log_stream.error(emphasize_single_message('Secondary input was UNRESOLVED. '
                                                        'Check optSecondaryDSs and/or optSecondaryDsTypes'))
              invalid = True
          tmpM = re.search('%%DS\d+%%', argStr)
          if tmpM:
              log_stream.error(emphasize_single_message('Unresolved placeholder {} in opt_args'.format(tmpM.group(0))))
              invalid = True
          print(json.dumps(x))
          if invalid:
              sys.exit(2)
      except Exception as e:
          log_stream.error(str(e) + traceback.format_exc())
          sys.exit(1)
