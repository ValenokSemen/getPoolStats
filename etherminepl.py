#!/usr/local/bin/python3
import requests as req
import json as js
import sys


class Eterminepl(object):
    """docstring for eterminepl"""

    payouts = None
    currentStats = None
    workers = None
    req_err = True

    def __init__(self, wallet):
        super(Eterminepl, self).__init__()
        self.wallet = wallet

    def get_all_response(self):
        self.set_workers()
        self.set_payouts()
        self.set_currentStats()
        self.set_response_status()    

    def __constuct_url(self, action):
        return 'https://api.ethermine.org/miner/{wallet}/{action}'.format(wallet=self.wallet, action=action)

    def _request(self, action):
        # Make a request to the API endpoint and refresh our data.
        # Return a 0 on success, otherwise return 1.
        response = req.get(self.__constuct_url(action))    
        try:
            if response.status_code != 200:
                # Raise HTTPError on bad status code
                response.raise_for_status()
            else:
                return response.json()
            # Got bad HTTP status code from API.
        except req.HTTPError as e:
            print("HTTPError exception: bad status code", file=sys.stderr)
            return 1
            # Generic connection error.
        except req.ConnectionError:
            print(
                "ConnectionError exception: are you connected to the internet?", file=sys.stderr)
            return 1
            # Request timed out.
        except req.Timeout:
            print("Timeout exception: Request timed out.", file=sys.stderr)
            return 1
            # Failure to decode JSON response.
        except js.JSONDecodeError:
            print("JSONDecodeError exception", file=sys.stderr)
            return 1
        else:
            return 0    

    def set_payouts(self):
        self.payouts = self._request('payouts')

    def set_currentStats(self):
        self.currentStats = self._request('currentStats')

    def set_workers(self):
        self.workers = self._request('workers')

    def set_response_status(self):
        if (self.payouts and self.currentStats and self.workers) == 1:
            self.req_err = False
        else:
            print("All data response")
            self.req_err = True

    def get_payouts(self):
        return self.payouts

    def get_currentStats(self):
        return self.currentStats

    def get_workers(self):
        return self.workers

    def get_response_status(self):
        return self.req_err
