class StockSymbolError(Exception):
    def __init__(self, stock_ymbol):
        self.msg = f"股票代码格式错误，请检查{stock_ymbol}"
    
    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return self.msg
    
class ParameterMissingError(Exception):
    def __init__(self, arg_name, arg_info):
        self.msg = f"缺失参数{arg_name},请检查后重新提交; {arg_name}参数：{arg_info}"
    
    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return self.msg
    
    
class InvalidParameterError(Exception):
    def __init__(self, arg_name, value, valid_value_list):
        self.msg = f"参数{arg_name}值{value}不合法，请检查后重新提交:"
        if valid_value_list:
            self.msg += f"; 可选参数:{','.join(valid_value_list)}"
    
    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return self.msg