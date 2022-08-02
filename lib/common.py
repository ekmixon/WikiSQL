def count_lines(fname):
    with open(fname) as f:
        return sum(1 for _ in f)


def detokenize(tokens):
    ret = ''.join(g + a for g, a in zip(tokens['gloss'], tokens['after']))
    return ret.strip()
