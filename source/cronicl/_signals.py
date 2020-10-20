"""
Signals are special messages which affect behaviour.

- TERMINATE queue handlers to stop processing new messages
- EMIT      collector operations should show their current value, 
            this does not mean the collector should reset
- RESET     collector operations should reset their value and start
            collecting again
"""

class Signals:

    TERMINATE = r'RUHV62H89O0rAYFXdFlgn_WxebDH2T2SOtCtoiONFdhL#M_-j#kLooj895vzuonG'
    EMIT = r'C2F1npw0kg=gh-qNli6CV5ftGHN=Q2loyzUX=N1kfiqqAYwIKYhpTo_vcqj003ch'
    RESET = r'iz8XeXWyZDDfxhrzjjKqUmV6EMnfwYDF1K7geSllCMY3lg=4JasLIVX2G9gs57cM'

    

