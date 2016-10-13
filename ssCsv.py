#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import cx_Oracle
import logging, sys, json, gzip, os, threading
import time, datetime, getopt
import gc



argv0=os.path.basename(sys.argv[0 ])

logger = logging.getLogger('')
logger.setLevel(logging.INFO)
format = logging.Formatter("%(asctime)s [%(relativeCreated)12d] - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch2 = logging.FileHandler('log/'+datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')+'_'+argv0+'.log')
ch.setFormatter(format)
ch2.setFormatter(format)
logger.addHandler(ch)
logger.addHandler(ch2)

logger.info('Hello you!')

### Input params ######################################################################
options, remainder = getopt.gnu_getopt(sys.argv[1:], 'c:d:', \
  [   'collectionToRun=',
      'debugLevel=',
  ])
# ARGS Default values
parCollectionToRun = '___ALL___'
parDebugLevel = 0

# Parse command line params
for opt, arg in options:
  if opt in ('-c', '--collectionToRun'):
    # What to run
    parCollectionToRun = arg

  elif opt in ('-d', '--debugLevel'):
    # Need debug?
    parDebugLevel = int(arg)


if parDebugLevel == 1:
  logger.setLevel(logging.DEBUG)


logger.debug('parCollectionToRun: %s', parCollectionToRun)
logger.debug('parDebugLevel: %s', parDebugLevel)

#######################################################################################





### Couple of functions here ##########################################################
# To avoid calculating replacement length every time
def cleanFieldLenInit ( paramConfig ):
  cnt = 0
  try:
    while len(paramConfig['replaceCharFrom'+str(cnt+1)]) > 0:
      cnt +=1
  except:
      pass # Who gives a hitch? :)
  return cnt+1

# Replace filed text based on configuration
def cleanField ( paramValue, paramConfig, parReplaceLength ):
  # Properly handle null values
  if paramValue == None:
    return ''

  # Just for strings (won't replace numbers...)
  if type(paramValue) is str:
    for idx in range(1,parReplaceLength):
      idx = str(idx)
      paramValue = paramValue.replace(paramConfig['replaceCharFrom'+idx], paramConfig['replaceCharTo'+idx])
  return paramValue
#######################################################################################







#######################################################################################
### This function runs queries in background ##########################################
#######################################################################################
def queryRunner ( queryIdx ):

  global cachedHeader
  global cachedResults

  logger.debug('- - This is query %s', str(queryIdx) )
  logger.debug('- - DB connection: %s', localConnectionString)
  logger.debug('- - cxOracle_arraySize: %s', str(localOptions['cxOracle_arraySize']))
  oracleConnection = cx_Oracle.connect(localConnectionString)

  logger.debug('Query:{%s}', queriesList[queryIdx]['query'] )

  cursor = oracleConnection.cursor()
  cursor.arraysize=int(localOptions['cxOracle_arraySize'])
  cursor.execute(queriesList[queryIdx]['query'])
  cachedHeader[queryIdx]=queriesList[queryIdx]['queryHeader']+','

  myResultData=list(list(cursor.fetchall()))

  cursor.close()
  oracleConnection.close()

  # Free some memory
  del cursor
  #del queryResults
  del oracleConnection
  gc.collect()


  # For each row in query
  for rowResultData in range(0, len(myResultData) ):
    resultId=myResultData[rowResultData][0]
    resultValues=myResultData[rowResultData]

    # With first query results, structures are initialized
    if queryIdx == 0:
      spoolData[resultId] = ''

      #for queryIdx2 in range(queryNum):
      #  cachedResults[queryIdx2][resultId]=''

      thisDataSet=resultValues

    else:
      # Don't waste time (and memory) with IDs which have not being spooled in first query
      if resultId not in cachedResults[0] and localOptions['allCollectionQueryRunParallel']=='N':
        del myResultData[rowResultData]
        break

      thisDataSet=resultValues[1:]

    # Free some memory
    del resultValues

    # For each column in row
    for thisVal in thisDataSet:

      if queriesList[queryIdx]['queryType']=='array':
        if thisVal == None:
          cachedResults[queryIdx][resultId] = cachedResults[queryIdx].get(resultId,'')+''
        else:
          cachedResults[queryIdx][resultId] = cachedResults[queryIdx].get(resultId,'')+str(  cleanField(thisVal, localOptions, cleanFieldLen)  )  +  localOptions['arraySeparator']

      else:
        if thisVal == None:
          cachedResults[queryIdx][resultId] = cachedResults[queryIdx].get(resultId,'')+','
        else:
          cachedResults[queryIdx][resultId] = cachedResults[queryIdx].get(resultId,'')+'"'+str(  cleanField(thisVal, localOptions, cleanFieldLen)   )+ '",'

    # Free some memory
    del thisDataSet
    myResultData[rowResultData]=''

  # Free some memory
  thisDataSet=None
  del myResultData
  logger.debug('- - DONE query %s', str(queryIdx) )
  gc.collect()
#######################################################################################
#######################################################################################
#######################################################################################






### MAIN configuration got read here ##################################################
globalConnectionString=[open(argv0+'.conn').read()][0]
for line in open(argv0+'.conn'):
  if line[0].strip() != '#' and line.strip() != '':
    globalConnectionString=line

globalOptions = {}
localOptions = {}
for line in open(argv0+'.conf'):
  if line[0].strip() != '#' and line.strip() != '':
    if line.find('#')>0:
      line=line[:line.find('#')].strip()
    (key, val) = line.split('=')
    globalOptions[key]=val.replace('\n','')
#######################################################################################






### Which collection to run? ##########################################################
if parCollectionToRun == "___ALL___":
  collectionFilesArray = ['collections/'+f for f in os.listdir('collections') if f.endswith('.json')]
else:
  collectionFilesArray = parCollectionToRun.split(',')
logger.debug('collectionFilesArray='+' | '.join(collectionFilesArray))
#######################################################################################







#######################################################################################
### Rock'n'roll! ######################################################################
for collectionFile in collectionFilesArray:
  # Get collection data
  collectionJson=open(collectionFile, 'r')
  jsonConfFile =json.load(collectionJson)
  queriesList = jsonConfFile['queries']
  outFileName = jsonConfFile['outFileName']
  collectionJson.close()

  localOptions = globalOptions
  if os.path.isfile(collectionFile+'.conf'):
    for line in open(collectionFile+'.conf'):
      if line[0].strip() != '#' and line.strip() != '':
        if line.find('#')>0:
          line=line[:line.find('#')].strip()
        (key, val) = line.split('=')
        localOptions[key]=val.replace('\n','')

  queryNum=len(queriesList)
  queryNumLoop=queryNum-1

  cleanFieldLen = cleanFieldLenInit (localOptions)

  # Get connection string related to thie collection, if any '.conn' file present
  localConnectionString = globalConnectionString
  if os.path.isfile(collectionFile+'.conn'):
    for line in open(collectionFile+'.conn'):
      if line[0].strip() != '#' and line.strip() != '':
        localConnectionString = line

  myResultData=[]
  cachedHeader=[[] for x in range(queryNum)]
  spoolData={}
  cachedResults=[dict() for x in range(queryNum)]

  threadList=[]


  logger.info('--- Now running collection %s', collectionFile)
  # I do this here, so if it breaks, it breaks *before* running long queries
  logger.info('Opening file %s', outFileName)
  fileOut = gzip.open(outFileName, 'wb')

  startTime = time.time()
  #######################################################################################
  # For each query in json file
  for queryIdx in range(0,queryNum):

    # Just one query, run it "serial"
    if queryIdx == 0  and localOptions['allCollectionQueryRunParallel']=='N':
      logger.info('- Executing query %i/%i', queryIdx+1,queryNum )
      queryRunner ( queryIdx )

    # Two queries, allCollectionQueryRunParallel==N: i'd run this in a thread if i don't have
    #  this condition; since it's second and *last* query, no need to thread it
    elif queryIdx == 1 and queryNum == 2 and localOptions['allCollectionQueryRunParallel']=='N':
      logger.info('- Executing query %i/%i', queryIdx+1,queryNum )
      queryRunner ( queryIdx )

    else:
      # Both allCollectionQueryRunParallel==Y
      # OR
      # More than 2 queries, first one executed "serial", i thread the remaining
      logger.info('- Queued query %i/%i', queryIdx+1,queryNum )
      thisThread = threading.Thread( target=queryRunner, args=(queryIdx,) )
      threadList.append(thisThread)

  # Starts threads
  for thisThread in threadList:
    thisThread.start()
    time.sleep(0.2)

  time.sleep(1)
  if queryNum > 2:
    logger.info('- Waiting for queries to end')
  for thisThread in threadList:
    thisThread.join()


  #######################################################################################
  # Here is where each single query results are "merged", so i will have a
  #  single string row for each ID(pk)
  if queryNum > 2:
    logger.info('- Merging query results')

  # Free some memory
  gc.collect()
  for queryIdx in range(0,queryNum):

    # Simple queryType
    if queriesList[queryIdx]['queryType'] != 'array':
      for resultId in spoolData:
        if not resultId in cachedResults[queryIdx]:
          spoolData[resultId]+=','
        else:
          spoolData[resultId]+=cachedResults[queryIdx][resultId]

        # Free some memory
        del cachedResults[queryIdx][resultId]

    # Array queryType
    else:

      for resultId in spoolData:
        if len(cachedResults[queryIdx][resultId]) > 0:
          spoolData[resultId]+='"'
      for resultId in spoolData:
        if len(cachedResults[queryIdx][resultId]) > 0:
          spoolData[resultId]+=cachedResults[queryIdx][resultId].rstrip(localOptions['arraySeparator'])

      for resultId in spoolData:
        if len(cachedResults[queryIdx][resultId]) > 0:
          spoolData[resultId]+='",'
        else:
          spoolData[resultId]+=','

        # Free some memory
        del cachedResults[queryIdx][resultId]

    # Free some more memory
    cachedResults[queryIdx] = None

  # Free some memory
  gc.collect()

  #######################################################################################
  # Same "merge" thing, but for header
  spoolHeader=''
  for queryIdx in range(queryNum):
    spoolHeader+=cachedHeader[queryIdx]
    cachedHeader[queryIdx]=''
  # Would love to get rid of this [:-1] to void printing last comma, but still didn't made it :(
  spoolHeader=spoolHeader[:-1]



  #######################################################################################
  ### Spool output
  #######################################################################################
  logger.info('Spooling now')
  fileOut = gzip.open(outFileName, 'wb')
  fileOut.write( spoolHeader  + '\n' )

  loopId = 0
  writeBuffer = ""
  rowsToSpool=len(spoolData)
  logger.debug('Spooling every %s rows', str(localOptions['syncWritesEvery']))
  for resultId in sorted(spoolData):

    writeBuffer += spoolData[resultId][:-1] + '\n'
    del spoolData[resultId]
    loopId +=1
    if loopId >= int(localOptions['syncWritesEvery']):
      logger.debug('Spooling')
      fileOut.write( writeBuffer )
      writeBuffer = ""
      loopId = 0
      #gc.collect()

  fileOut.write( writeBuffer )
  fileOut.close()

  # Free some memory
  del spoolData
  del writeBuffer
  del spoolHeader
  gc.collect()

  logger.info('Ended elaborating %s; %s rows in %f seconds ', collectionFile, str(rowsToSpool), time.time()-startTime )

logger.info('Have a nice day!')
