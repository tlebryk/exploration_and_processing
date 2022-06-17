political_article = '''White House declares war against terror. The US government officially announced a ''' \
                        '''large-scale military offensive against terrorism. Today, the Senate agreed to spend an ''' \
                        '''additional 300 billion dollars on the advancement of combat drones to be used against ''' \
                        '''global terrorism. Opposition members sharply criticize the government. ''' \
                        '''"War leads to fear and suffering. ''' \
                        '''Fear and suffering is the ideal breeding ground for terrorism. So talking about a ''' \
                        '''war against terror is cynical. It's actually a war supporting terror."'''
nonpolitical_article = '''Table tennis world cup 2025 takes place in South Korea. ''' \
                           '''The 2025 world cup in table tennis will be hosted by South Korea, ''' \
                           '''the Table Tennis World Commitee announced yesterday. ''' \
                           '''Three-time world champion, Hu Ho Han, did not pass the qualification round, ''' \
                           '''to the advantage of underdog Bob Bobby who has been playing outstanding matches ''' \
                           '''in the National Table Tennis League this year.'''

# from polinews import Classifier
import polinews
from polinews import polinews
classifier = Classifier()
probabilities = classifier.estimate([political_article, nonpolitical_article])
probabilities[0] > 0.99
probabilities[1] < 0.01
