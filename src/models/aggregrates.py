from src.models.db import BaseModel

class Aggregates(BaseModel):
  def __init__(self):
    BaseModel.__init__(self)
    self.table_name = 'aggregates'
    self.abbr = 'ag'
    self.fields = {
      'id': 'int',
      'ticker': 'string',
      'underlying': 'string',
      'date': 'date',
      'open': 'float',
      'close': 'float',
      'high': 'float',
      'low': 'float',
      'volume': 'int',
      'vw': 'float',
      'number': 'int'
    }
