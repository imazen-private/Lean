### <summary>
### Basic Template Library Class
###
### Library classes are snippets of code/classes you can reuse between projects. They are
### added to projects on compile. This can be useful for reusing indicators, math functions,
### risk modules etc. Make sure you import the class in your algorithm. You need
### to name the file the module you'll be importing (not main.cs).
### importing.
### </summary>
class BasicTemplateLibrary:

    '''
    To use this library place this at the top:
    from BasicTemplateLibrary import BasicTemplateLibrary

    Then instantiate the function:
    x = BasicTemplateLibrary()
    x.Add(1,2)
    '''
    def Add(self, a, b):
        return a + b

    def Subtract(self, a, b):
        return a - b
