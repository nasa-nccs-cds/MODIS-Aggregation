from stratus_endpoint.handler.base import Endpoint, TaskHandle, Status, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
import traceback
import atexit, ast, os, json
from edas.portal.base import Message, Response
from typing import Dict, Any, Sequence
from source.xarray.MODIS_Aggregation import MODIS_L2_L3_Aggregation

def get_or_else( value, default_val ): return value if value is not None else default_val

class ModisAggEndpoint(Endpoint):

    def __init__(self, **kwargs ):
        super(ModisAggEndpoint, self).__init__()
        self.logger =  self.getLogger()
        self.process = "modis-agg"
        self.handlers = {}
        self._epas = [ "modis-agg" ]
        atexit.register( self.shutdown, "ShutdownHook Called" )

    def epas( self ) -> List[str]: return self._epas

    def init( self ):
        self.aggregate = MODIS_L2_L3_Aggregation()
        self.outputFile = self.aggregate.CloudFraction_Daily_Aggregation(satellite="Aqua", date="2008001")

    def capabilities(self, type: str, **kwargs  ) -> Dict:
        if type == "epas":
            return dict( epas = self._epas )
        else: raise Exception( f"Unknown capabilities type: {type}" )

    def sendErrorReport( self, clientId: str, responseId: str, msg: str ):
        self.logger.info("@@Portal-----> SendErrorReport[" + clientId +":" + responseId + "]: " + msg )

    def sendFile( self, clientId: str, jobId: str, name: str, filePath: str, sendData: bool ):
        self.logger.debug( "@@Portal: Sending file data to client for {}, filePath={}".format( name, filePath ) )

    def request(self, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs ) -> TaskHandle:
        rid = requestSpec.get( "rid" )
        cid = requestSpec.get( "cid" )
        self.logger.info( f"EDAS Endpoint--> processing rid {rid}")
        try:
          satellite = requestSpec.get("satellite")
          date = requestSpec.get("date" )
          self.aggregate.CloudFraction_Daily_Aggregation( satellite, date )
          execHandler: ExecHandler = self.addHandler( rid, ExecHandler( cid, job ) )
          execHandler.execJob( job )
          return execHandler
        except Exception as err:
            self.logger.error( "Caught execution error: " + str(err) )
            traceback.print_exc()
            return TaskHandle( rid=rid, cid=cid, status = Status.ERROR, error = ExecHandler.getErrorReport( err ) )

    def shutdown( self, *args ):
        print( "Shutdown: " + str(args) )

