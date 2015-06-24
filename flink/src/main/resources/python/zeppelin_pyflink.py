#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys, getopt, traceback

from py4j.java_gateway import java_import, JavaGateway, GatewayClient
from py4j.protocol import Py4JJavaError

client = GatewayClient(port=int(sys.argv[1]))
gateway = JavaGateway(client)

intp = gateway.entry_point
intp.onPythonScriptInitialized()

class Logger(object):
  def __init__(self):
    self.out = ""

  def write(self, message):
    self.out = self.out + message

  def get(self):
    return self.out

  def reset(self):
    self.out = ""

try:
  z = intp.getZeppelinContext()

  output = Logger()
  sys.stdout = output
  sys.stderr = output

  while True :
    req = intp.getStatements()
    try:
      stmts = req.statements().split("\n")
      jobGroup = req.jobGroup()
      final_code = None

      for s in stmts:
        if s == None or len(s.strip()) == 0:
          continue

        # skip comment
        if s.strip().startswith("#"):
          continue

        if final_code:
          final_code += "\n" + s
        else:
          final_code = s

      if final_code:
        compiledCode = compile(final_code, "<string>", "exec")
        eval(compiledCode)

      intp.setStatementsFinished(output.get(), False)
    except Py4JJavaError:
      excInnerError = traceback.format_exc() # format_tb() does not return the inner exception
      innerErrorStart = excInnerError.find("Py4JJavaError:")
      if innerErrorStart > -1:
        excInnerError = excInnerError[innerErrorStart:]
      intp.setStatementsFinished(excInnerError + str(sys.exc_info()), True)
    except:
      intp.setStatementsFinished(str(sys.exc_info()), True)

    output.reset()

except:
  intp.setStatementsFinished(str(sys.exc_info()), True)

