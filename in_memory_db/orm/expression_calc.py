
class ExpressionCalculate:
    
    def __init__(self):
        self.literals = ['*',  '+', '/', '-', '%', '^']
        self.operations = {
            '*': lambda x, y : x*y,
            '+': lambda x, y : x+y,
            '-': lambda x, y : x-y,
            '/': lambda x, y : x/y,
            '%': lambda x, y : x%y,
            '^': lambda x, y : x^y,
        }
    @staticmethod
    def calc_precision(expr):

        if expr == '^':
            return 3
        elif expr == '/' or expr == '*':
            return 2
        elif expr == '+' or expr == '-':
            return 1
        else:
            return -1

    def infix_to_postfix(self, expression):
        stack = []
        result = []

        for i in range(len(expression)):
            expr = expression[i]

            if ('a' <= expr <= 'z') or ('A' <= expr <= 'Z') or ('0' <= expr <= '9'):
                result.append(expr)

            elif expr == '(':
                stack.append('(')

            elif expr == ')':
                while stack[-1] != '(':
                    result.append(stack.pop())
                stack.pop()

            else:
                while stack and (self.calc_precision(expr) < self.calc_precision(stack[-1]) or self.calc_precision(expr) == self.calc_precision(stack[-1])):
                    result.append(stack.pop())
                stack.append(expr)

        while stack:
            result.append(stack.pop())

        return result

    def tokenize(self, expression=''):
        start_pt = 0
        tokens=[]
        temp_expr = ''
        while start_pt < len(expression):
            
            if expression[start_pt] in ('(', ')'):
                if temp_expr:
                    tokens.append(temp_expr)
                    
                tokens.append(expression[start_pt])
                temp_expr = ''

            elif expression[start_pt] in self.literals:
                if temp_expr:
                    tokens.append(temp_expr)
                    
                tokens.append(expression[start_pt])
                temp_expr = ''
            
            elif expression[start_pt] == ' ':
                pass
            
            else:
                temp_expr += expression[start_pt]
            
            start_pt+=1
        
        if temp_expr:
            tokens.append(temp_expr)
            
        return tokens

    def calculate_postfix(self, postfix_tokens, data, headers):
        stack=[]
        for char in postfix_tokens:
            if '0' <= char <= '9':
                stack.append(int(char))
            
            elif char in headers:
                stack.append(data[headers[char]])
            
            elif char in self.literals:
                operand_1 = stack.pop(-1)
                operand_2 = stack.pop(-1)
                val = self.operations[char](operand_2, operand_1)
                stack.append(val)
        
        return stack[0]
