from src.models.db import BaseModel

class Contracts(BaseModel):
  def __init__(self):
    BaseModel.__init__(self)
    self.table_name = 'contracts'
    self.abbr = 'c'
    self.fields = {
      'id': 'int',
      'ticker': 'string',
      'underlying': 'string',
    }

  def contractsForDate(date):
    pass