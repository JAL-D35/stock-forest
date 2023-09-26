from json import dumps
from urllib.parse import urlencode

from requests import get
from xmltodict import parse


class PublicData:
    def __init__(
        self,
        url: str,
        result_type: str,
        mrkt_cls: str,
        num_of_rows: str,
        page_no: str,
        bas_dt: str,
    ):
        self.url = url
        self.result_type = result_type
        self.mrkt_cls = mrkt_cls
        self.num_of_rows = num_of_rows
        self.page_no = page_no
        self.bas_dt = bas_dt

    def _generate_params(self):
        params = {}
        params["resultType"] = self.result_type
        params["mrktCls"] = self.mrkt_cls
        params["numOfRows"] = self.num_of_rows
        params["pageNo"] = self.page_no
        params["basDt"] = self.bas_dt
        return params

    def _edit_url(self):
        return f"{self.url}&{urlencode(self._generate_params())}"

    def _get_data(self):
        return get(self._edit_url()).text

    def _data_to_dict(self):
        return parse(self._get_data())

    def _dict_to_json(self):
        try:
            return [
                dumps(
                    self._data_to_dict()["response"]["body"]["items"]["item"], indent=4
                )
            ]
        except (KeyError, TypeError):
            return

    def load(self):
        return self._dict_to_json()
