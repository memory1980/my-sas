class Dog:

    kind = 'canine'  
    
    today='星期一'# 类变量被所有实例所共享

    def __init__(self, name):
        self.name = name    # 实例变量为每个实例所独有

d = Dog('Fido')
e = Dog('Buddy')

print(d.today)                  # 被所有的 Dog 实例所共享
# 'canine'
print(e.kind )                 # 被所有的 Dog 实例所共享
# 'canine'
print(d.name     )             # 为 d 所独有
# 'Fido'
print(e.name         )         # 为 e 所独有
# 'Buddy'