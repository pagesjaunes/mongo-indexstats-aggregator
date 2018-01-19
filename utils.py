# -*-coding:UTF-8 -*
import sys
import locale

# variables stockant l'état actuel du niveau de log => reinit plus tard dans le main
isTrace = False
isDebug = False

# pour le debugging
def set_debug_level(isDebugOrTraceEnabled):
    global isDebug
    isDebug = isDebugOrTraceEnabled

def log_debug(msg):
    global isDebug
    if isDebug :
        msg = "--- DEBUG --- " + msg + "\n"
        sys.stdout.write(msg)
        sys.stdout.flush()

# pour le tracing
def set_trace_level(isTraceEnabled):
    global isTrace
    isTrace = isTraceEnabled

def log_trace(msg, withEndOfLine=True):
    global isTrace
    if isTrace :
        msg = "--- TRACE --- " + msg
        if withEndOfLine :
            msg = msg + "\n"
        sys.stdout.write(msg)
        sys.stdout.flush()

# pour le logging standard
def log(msg):
    sys.stdout.write(msg)
    sys.stdout.flush()

def log_retourchariot(msg):
    log(msg + "\n")

# sortir en erreur
def log_erreur(msg):
    log(msg)
    sys.exit(1)

def humanize_int(nb):
    # utilisation de la local US car les séparteurs des milliers est une virgule
    # => on la remplace ensuite par un espace
    # et on reinit la locale
    locale.setlocale(locale.LC_ALL, 'en_US')
    res = locale.format('%d', nb, grouping=True).replace(",", " ")
    log_debug("res=" + res)
    locale.resetlocale(locale.LC_ALL)
    return res

# lancement de ce script => NE FAIT QUE DES TESTS des fonctions dispos
if __name__ == '__main__':
    sys.stdout.write("Lancement batteries de Tests pour le module UTILS\n")

    sys.stdout.write("\ntest1 => vérif fonction log\n")
    log("---TEST LOG---")

    sys.stdout.write("\ntest2 => vérif fonction log_retourchariot\n")
    log_retourchariot("---TEST LOG RETOUR CHARIOT---")

    sys.stdout.write("\ntest3 => vérif fonction log_debug en mode DISABLED\n")
    log_debug("---TEST LOG_DEBUG (dsiabled)---")

    sys.stdout.write("\ntest4 => vérif fonction log_debug en mode ENABLED\n")
    sys.stdout.write("\ntest4 => AVANT modif debug level => DEBUG="+str(isDebug)+"\n")
    set_debug_level(True)
    sys.stdout.write("\ntest4 => APRES modif debug level => DEBUG="+str(isDebug)+"\n")
    log_debug("---TEST LOG_DEBUG (enabled)---")

    sys.stdout.write("\ntest5 => vérif fonction log_trace en mode DISABLED\n")
    log_trace("---TEST LOG_TRACE (dsiabled)---")

    sys.stdout.write("\ntest6 => vérif fonction log_trace en mode ENABLED\n")
    set_trace_level(True)
    log_trace("---TEST LOG_TRACE (enabled)---")

    sys.stdout.write("\ntest7 => vérif fonction humanize_int en mode DEBUG\n")
    nb = 12345678.90
    nbStr1 = humanize_int(nb)
    sys.stdout.write("\nresultat = " + nbStr1 + "\n")
    assert nbStr1 == "12 345 678"

    sys.stdout.write("\ntest8 => vérif fonction humanize_int en mode NON DEBUG\n")
    set_debug_level(None)
    nbStr2 = humanize_int(nb)
    sys.stdout.write("\nresultat = " + nbStr2 + "\n")
    assert nbStr2 == "12 345 678"

    sys.stdout.write("\ntest9 => vérif fonction log_erreur\n")
    log_erreur("---TEST ERREUR---")
    assert False
