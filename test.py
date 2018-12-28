import pandas as pd
import Configuration as cfg

def getCallType(id):
    callTypes = [{cfg.ID_PROPERTY: cfg.CODE_LOCAL_MESSAGE, cfg.NAME_PROPERTY: cfg.NAME_LOCAL_MESSAGE},
                 {cfg.ID_PROPERTY: cfg.CODE_INTERLATA,     cfg.NAME_PROPERTY: cfg.NAME_INTERLATA},
                 {cfg.ID_PROPERTY: cfg.CODE_INCOMING_CDR,  cfg.NAME_PROPERTY: cfg.NAME_INCOMING_CDR}]
    return next((x[cfg.NAME_PROPERTY] for x in callTypes if x[cfg.ID_PROPERTY] == id), None)

def calculateTime(time):
    if cfg.COLON in time:
        minutes = str(time).split(":")[0]
        seconds = str(time).split(":")[1]
        minutesToMilliseconds = (float(minutes)) * cfg.MINUTES * cfg.MILLISECONDS
        secondsToMilliseconds =  float(seconds) * cfg.MILLISECONDS
        return minutesToMilliseconds + secondsToMilliseconds
    return time

def transform():
    dataFrame = pd.read_csv(cfg.FILENAME, sep=cfg.SEPARATOR)
    copyDatatFrame = dataFrame[ cfg.AVAILABLE_KEYS ].copy()
    copyDatatFrame[cfg.COLUMN_TYPE] = dataFrame[cfg.COLUMN_CALL_TYPE].apply(lambda x : getCallType(x))
    normalizeTrunkID(copyDatatFrame, dataFrame)
    normalizeTrunkGroupInfo(dataFrame, copyDatatFrame)
    normalizeNumbers(dataFrame, copyDatatFrame)
    copyDatatFrame[cfg.COLUMN_ELAPSED_TIME] = dataFrame[cfg.COLUMN_LENGTH_CALL].apply(lambda x : calculateTime(str(x)))
    copyDatatFrame.to_csv( cfg.OUTPUT_FILENAME, index=False, sep=cfg.SEPARATOR )

def normalizeTrunkID(copyDatatFrame, dataFrame):
    dataFrame["TrunkIRI0"], dataFrame["TrunkIRI1"] = dataFrame[cfg.COLUMN_TRUNK_IRI].str.replace("\n", "").str.strip().str.split(",").str
    dataFrame["TrunkIGN0"], dataFrame["TrunkIGN1"] = dataFrame[cfg.COLUMN_TRUNK_IGN].str.replace("\n","").str.strip().str.split(",").str
    dataFrame["TrunkITM0"], dataFrame["TrunkITM1"] = dataFrame[cfg.COLUMN_TRUNK_ITM].str.replace("\n", "").str.strip().str.split(",").str
    copyDatatFrame[cfg.COLUMN_M104_TRUNK0_ID] = dataFrame["TrunkIRI0"].astype(str) + ":" + dataFrame["TrunkIGN0"].astype(str) + ":" + dataFrame["TrunkITM0"].astype(str)
    copyDatatFrame[cfg.COLUMN_M104_TRUNK1_ID] = dataFrame["TrunkIRI1"].astype(str) + ":" + dataFrame["TrunkIGN1"].astype(str) + ":" + dataFrame["TrunkITM1"].astype(str)
    copyDatatFrame[cfg.COLUMN_M104_TRUNK0_ID] = copyDatatFrame[cfg.COLUMN_M104_TRUNK0_ID].str.replace( cfg.NAN_VALUE, "")
    copyDatatFrame[cfg.COLUMN_M104_TRUNK1_ID] = copyDatatFrame[cfg.COLUMN_M104_TRUNK1_ID].str.replace( cfg.NAN_VALUE, "")

def normalizeNumbers(df, df_new):
    df_new[cfg.COLUMN_ORIGIN_NPA] = df[cfg.COLUMN_CALLING_NUMBER].str[:3]
    df_new[cfg.COLUMN_ORIGIN_NUMBER] = df[cfg.COLUMN_CALLING_NUMBER].str[4:12]
    df_new[cfg.COLUMN_TERMINATING_NPA] = df[cfg.COLUMN_CALLED_NUMBER].str[:3]
    df_new[cfg.COLUMN_TERMINATING_NUMBER] = df[cfg.COLUMN_CALLED_NUMBER].str[4:12]

def normalizeTrunkGroupInfo(df, df_new):
    df_new[cfg.COLUMN_M119_TRUNK_GROUP_INFO] = df[cfg.COLUMN_TRUNK_GROUP_INTEROFFICE].astype(str)
    df_new[cfg.COLUMN_M119_TRUNK_GROUP_INFO] = df_new[cfg.COLUMN_M119_TRUNK_GROUP_INFO].str.replace("nan", "")

transform()