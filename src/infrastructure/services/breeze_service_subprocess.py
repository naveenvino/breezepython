"""
Breeze API Service - Subprocess Solution
Runs Breeze API calls in a separate process to avoid async/thread issues
"""
import logging
import json
import subprocess
import sys
from datetime import datetime
from typing import Dict, Optional, Any
import os
from pathlib import Path

from ...config.settings import get_settings

logger = logging.getLogger(__name__)


class BreezeServiceSubprocess:
    """Breeze service that uses subprocess to avoid async issues"""
    
    def __init__(self):
        self.settings = get_settings()
        # Create the worker script path
        self.worker_script = Path(__file__).parent / "breeze_worker.py"
        self._create_worker_script()
    
    def _create_worker_script(self):
        """Create the worker script that will run in subprocess"""
        worker_code = '''
import sys
import json
import os
from datetime import datetime
from breeze_connect import BreezeConnect

def main():
    # Get command from stdin
    command = json.loads(sys.stdin.read())
    
    # Initialize Breeze
    breeze = BreezeConnect(api_key=command['api_key'])
    try:
        breeze.generate_session(
            api_secret=command['api_secret'],
            session_token=command['session_token']
        )
    except Exception as e:
        # Continue even if customer details error
        pass
    
    # Execute the request
    try:
        if command['type'] == 'historical_data':
            result = breeze.get_historical_data_v2(
                interval=command['interval'],
                from_date=command['from_date'],
                to_date=command['to_date'],
                stock_code=command['stock_code'],
                exchange_code=command['exchange_code'],
                product_type=command['product_type'],
                expiry_date=command.get('expiry_date'),
                right=command.get('right'),
                strike_price=command.get('strike_price')
            )
            print(json.dumps(result))
        else:
            print(json.dumps({"Error": "Unknown command type"}))
    except Exception as e:
        print(json.dumps({"Error": str(e), "Success": []}))

if __name__ == "__main__":
    main()
'''
        
        # Write the worker script
        with open(self.worker_script, 'w') as f:
            f.write(worker_code)
    
    async def get_historical_data(
        self,
        interval: str,
        from_date: datetime,
        to_date: datetime,
        stock_code: str,
        exchange_code: str = "NSE",
        product_type: str = "cash",
        expiry_date: Optional[str] = None,
        right: Optional[str] = None,
        strike_price: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get historical data using subprocess"""
        
        try:
            # Prepare command
            command = {
                'type': 'historical_data',
                'api_key': self.settings.breeze.api_key,
                'api_secret': self.settings.breeze.api_secret,
                'session_token': self.settings.breeze.session_token,
                'interval': interval,
                'from_date': from_date.strftime("%Y-%m-%dT00:00:00.000Z"),
                'to_date': to_date.strftime("%Y-%m-%dT23:59:59.000Z"),
                'stock_code': stock_code,
                'exchange_code': exchange_code,
                'product_type': product_type
            }
            
            # Add optional parameters
            if expiry_date:
                command['expiry_date'] = expiry_date
            if right:
                command['right'] = right
            if strike_price:
                command['strike_price'] = strike_price
            
            logger.info(f"Fetching {stock_code} data using subprocess")
            
            # Run in subprocess
            process = subprocess.Popen(
                [sys.executable, str(self.worker_script)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Send command and get result
            stdout, stderr = process.communicate(input=json.dumps(command))
            
            if stderr:
                logger.error(f"Subprocess error: {stderr}")
            
            # Parse result
            try:
                result = json.loads(stdout)
                if 'Success' in result and result['Success']:
                    logger.info(f"Fetched {len(result['Success'])} records")
                return result
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse result: {stdout}")
                return {"Error": f"Parse error: {e}", "Success": []}
                
        except Exception as e:
            logger.error(f"Subprocess execution error: {e}")
            return {"Error": str(e), "Success": []}