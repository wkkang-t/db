import sys
import math
import operator
def transition_para(file):
    with open(file, 'r') as f:
        lines = f.readlines()
    start = 'START'
    stop = 'STOP'
    state_u = 'START'
    state_v = 'START'
    transition_parameters = {}
    for line in lines:
        split_line = line.strip()
        split_line = split_line.rsplit(" ", 1)
        if len(split_line) == 2:
            word = split_line[0]
            state_w = split_line[1]
            if state_u not in transition_parameters:
                transition_parameters[state_u] = {}
            if state_v not in transition_parameters[state_u]:
                state_v_dict = {}
            else:
                state_v_dict = transition_parameters[state_u][state_v]
            if state_w in state_v_dict:
                state_v_dict[state_w] += 1
            else:
                state_v_dict[state_w] = 1
            transition_parameters[state_u][state_v]= state_v_dict
            state_u = state_v
            state_v = state_w
        if len(split_line) != 2:
            if state_u not in transition_parameters:
                transition_parameters[state_u] ={}
            if state_v not in transition_parameters[state_u]:
                state_v_dict = {}
            else:
                state_v_dict = transition_parameters[state_u][state_v]
            state_w = stop
            if state_w in state_v_dict:
                state_v_dict[state_w] += 1
            else:
                state_v_dict[state_v] = 1
            transition_parameters[state_u][state_v] = state_v_dict
            state_u = start
            state_v = start
    return transition_parameters            

def emission_para(file):
    with open(file, 'r') as f:
        lines = f.readlines()
    words = set()
    emission_parameters = {}
    for line in lines:
        split_line = line.strip()
        split_line = split_line.rsplit(" ", 1)
        if len(split_line) == 2:
            word = split_line[0]
            state = split_line[1]
            words.add(word)
            if state in emission_parameters:
                current_emission = emission_parameters[state]
            else:
                current_emission = {}
            if word in current_emission:
                current_emission[word] = current_emission[word] +1
            else:
                current_emission[word] = 1
            emission_parameters[state] = current_emission
    return words, emission_parameters

def transition_count(transition_parameters, state_u, state_v, state_w):
    if state_u not in transition_parameters:
        count = 0
    elif state_v not in transition_parameters[state_u]:
        count = 0
    else:
        state_v_dict = transition_parameters[state_u][state_v]
        count_uvw = state_v_dict.get(state_w, 0)
        count_uv = sum(state_v_dict.values())
        count = count_uvw/count_uv
    return count

def emission_count(emission_parameters, word, state_w, k = 1):
    state_dict = emission_parameters[state_w]
    count_w = sum(state_dict.values()) + k
    if word != "#UNK#":
        count_wx = state_dict[word]
    else:
        count_wx = k
    return count_wx/count_w


def viterbi(emission_parameters, transition_parameters, words, sentence):
    n = len(sentence)
    min = - sys.maxsize -1
    token = "#UNK#"
    states = list(transition_parameters.keys())
    states.remove("START")
    scores = {}
    scores[0] = {}
    for state_w in states:
        transition_counts = transition_count(transition_parameters, "START", "START", state_w)
        if transition_counts != 0 :
            transition = math.log(transition_counts)
        else:
            transition = min
        if sentence[0] not in words:
            word = token
        else:
            word = sentence[0]
        if (( word in emission_parameters[state_w]) or (word == token)):
            emission_counts = emission_count(emission_parameters, word, state_w)
            emission = math.log(emission_counts)
        else:
            emission = min
        start_score = transition + emission
        scores[0][state_w] = {}
        scores[0][state_w]["START"] = {"START": start_score}
    if n == 1:
        scores[n] = {}
        scores[n]["STOP"] ={}
        scores[n]["STOP"]["START"] = {}
        for state_v in states:
            transition_counts = transition_count(transition_parameters, "START", state_v, "STOP")
            if transition_counts != 0:
                transition = math.log(transition_counts)
            else:
                transition = min
            score_v = scores[0][state_v]["START"]["START"]
            current_score = score_v + transition
            scores[n]["STOP"]["START"][state_v] = current_score
        path = ["STOP"]
        stop_lst = []
        for state_v in scores[n]["STOP"]["START"]:
            stop_lst.append((state_v,scores[n]["STOP"]["START"][state_v]))
        max_state_v = max(stop_lst, key = operator.itemgetter(1))
        path.insert(0, max_state_v[0])
        path.insert(0, "START")
        return path
    else:
        scores[1] ={}
        for state_v in states:
            scores[1][state_w] = {}
            scores[1][state_w]["START"] = {}
            for state_v in states:
                transition_counts = transition_count(transition_parameters, "START", state_v, state_w)
                if transition_counts!=0:
                    transition = math.log(transition_counts)
                else:
                    transition = min
                if sentence[1] not in words:
                    word = token
                else:
                    word = sentence[1]
                if((word in emission_parameters[state_w]) or (word == token)):
                    emission_counts = emission_count(emission_parameters, words, state_w)
                    emission = math.log(emission_counts)
                else:
                    emission = min
                current_score = scores[0][state_v]["START"]["START"] + transition + emission
                scores[1][state_w]["START"][state_v] = current_score
    if n == 2:
        scores[2] = {}
        scores[2]["STOP"] = {}
        for state_u in states:
            scores[n]["STOP"][state_u] = {}
            for state_v in states:
                transition_counts = transition_count(transition_parameters, state_u, state_v, 'STOP')
                if transition_counts != 0:
                    transition = math.log(transition_counts)
                else:
                    transition = min
                state_v_arr = []
                for old_state_u in scores[n-1][state_v]:
                    state_v_arr.append(scores[n-1][state_v][old_state_u][state_u])
                best_score_v = max(state_v_arr)
                current_stop_score = best_score_v + transition
                scores[n]["STOP"][state_u][state_v] = current_stop_score
        path = ["STOP"]
        stop_lst = []
        for state_u in scores[n]["STOP"]:
            state_u_lst = []
            for state_v in scores[n]["STOP"][state_u]:
                state_u_lst.append((state_v, scores[n]["STOP"][state_u][state_v]))
            max_state_v_for_state_u = max(state_u_lst, key=operator.itemgetter(1))
            max_tuple = (state_u, max_state_v_for_state_u[0], max_state_v_for_state_u[1])
            stop_lst.append(max_tuple)
        max_stop_tuple = max(stop_lst, key=operator.itemgetter(2))
        path.insert(0, max_stop_tuple[1])
        path.insert(0, max_stop_tuple[0])
        prev = -2
        for k in range(n-1, 0, -1):
            state_u = scores[k][path[prev]] 
            state_v_lst = []
        
            for i in state_u.keys():
                if path[prev-1] in state_u[i]:
                    state_v_lst.append((i, state_u[i][path[prev-1]]))
            max_score = max(state_v_lst, key=operator.itemgetter(1))
            prev = prev - 1
            path.insert(0, max_score[0])
    elif n > 2:
        scores[2] = {}
        for state_w in states:
            scores[2][state_w] = {}
            for state_u in states:
                scores[2][state_w][state_u] = {}
                for state_v in states:
                    transition_counts = transition_count(transition_parameters, state_u, state_v, state_w)
                    if transition_counts != 0:
                        transition = math.log(transition_counts)
                    else:
                        transition = min
                    if sentence[2] not in word:
                        word = token
                    else:
                        word = sentence[2]
                    if ((word in emission_parameters[state_w]) or (word == token)):
                        emission_counts = emission_count(emission_parameters, word, state_w)
                        emission = math.log(emission_counts)
                    else:
                        emission = min
                    current_score = scores[1][state_v]['START'][state_u] + transition + emission
                    scores[2][state_w][state_u][state_v] = current_score
        for i in range(3,n):
            scores[i] = {}
            for state_w in states:
                scores[i][state_w] = {}
                for state_u in states:
                    scores[i][state_w][state_u] = {}
                    for state_v in states:
                        transition_counts = transition_count(transition_parameters, state_u, state_v, state_w)
                        if transition_counts != 0:
                            transition = math.log(transition_counts)
                        else:
                            transition = min
                        if sentence[i] not in words:
                            word = token
                        else:
                            word = sentence[i]
                        if ((word in emission_parameters[state_w]) or (word == token)):
                            emission_counts = emission_count(emission_parameters, word, state_w)
                            emission = math.log(emission_counts)
                        else:
                            emission = min
                        state_v_arr = []
                        for old_state_u in scores[i-1][state_v]:
                            state_v_arr.append(scores[i-1][state_v][old_state_u][state_u])
                        best_score_v = max(state_v_arr)
                        current_score = best_score_v + transition + emission
                        scores[i][state_w][state_u][state_v] = current_score
        scores[n] = {}
        scores[n]["STOP"] = {}
        for state_u in states:
            scores[n]["STOP"][state_u] = {}
            for state_v in states:
                transition_counts = transition_count(transition_parameters, state_u, state_v, 'STOP')
                if transition_counts != 0:
                    transition = math.log(transition_counts)
                else:
                    transition = min
                state_v_arr = []
                for old_state_u in scores[n-1][state_v]:
                    state_v_arr.append(scores[n-1][state_v][old_state_u][state_u])
                
                best_score_v = max(state_v_arr)
                current_stop_score = best_score_v + transition
                scores[n]["STOP"][state_u][state_v] = current_stop_score
        path = ["STOP"]
        stop_lst = []
        for state_u in scores[n]["STOP"]:
            state_u_lst = []
            for state_v in scores[n]["STOP"][state_u]:
                state_u_lst.append((state_v, scores[n]["STOP"][state_u][state_v]))
            max_state_v_for_state_u = max(state_u_lst, key=operator.itemgetter(1))
            max_tuple = (state_u, max_state_v_for_state_u[0], max_state_v_for_state_u[1])
            stop_lst.append(max_tuple)
        max_stop_tuple = max(stop_lst, key=operator.itemgetter(2))
        path.insert(0, max_stop_tuple[1])
        path.insert(0, max_stop_tuple[0])
        prev = -2
        for k in range(n-1, 0, -1):
            state_u = scores[k][path[prev]]
            state_v_lst = []
            for i in state_u.keys():
                if path[prev-1] in state_u[i]:
                    state_v_lst.append((i, state_u[i][path[prev-1]]))
            max_score = max(state_v_lst, key=operator.itemgetter(1))
            prev = prev - 1
            path.insert(0, max_score[0])
    return path

if __name__ == '__main__':
    es_training = "RU/train"
    es_testing = "RU/dev.in"
    transition_parameters = transition_para(es_training)
    words, emission_parameters = emission_para(es_training)
    with open(es_testing) as f:
        lines = f.readlines()
    sentence = []
    prediction_list = []
    for line in lines:
        if line != "\n":
            line = line.strip()
            sentence.append(line)
        else:
            sentence_predict = viterbi(emission_parameters, transition_parameters, words, sentence)
            sentence_predict.remove("START")
            sentence_predict.remove("STOP")
            prediction_list = prediction_list + sentence_predict
            prediction_list = prediction_list + ["\n"]
            sentence = []
    print(prediction_list)