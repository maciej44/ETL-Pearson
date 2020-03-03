        print(self.__test.head(4))
        print(list(self.__final_df.columns.values))

# print(self.__final_df.has_student_with_scored_test.unique())


'''
class A(object):

    def __init__(self):
        print("Initialiser A was called")

class B(A):

    def __init__(self):
        A.__init__(self)
        # A.__init__(self,<parameters>) if you want to call with parameters
        print("Initialiser B was called")

class C(B):

    def __init__(self):
        #A.__init__(self) # if you want to call most super class...
        B.__init__(self)
        print("Initialiser C was called")


test = C()
'''