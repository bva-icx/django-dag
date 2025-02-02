# dict recurse template tag for django
# from http://djangosnippets.org/snippets/1974/

from django import template

register = template.Library()


class RecurseDictNode(template.Node):
    def __init__(self, var, nodeList):
        self.var = var
        self.nodeList = nodeList

    def __repr__(self):
        return '<RecurseDictNode>'

    def renderCallback(self, context, vals, level):
        if len(vals) == 0:
            return ''

        output = []

        if 'loop' in self.nodeList:
            output.append(self.nodeList['loop'].render(context))

        for k, v in vals:
            context.push()

            context['level'] = level
            context['key'] = k

            if 'value' in self.nodeList:
                output.append(self.nodeList['value'].render(context))

                if type(v) == list or type(v) == tuple:
                    child_items = [(None, x) for x in v]
                    output.append(self.renderCallback(
                        context, child_items, level + 1))
                else:
                    try:
                        child_items = v.items()
                        output.append(self.renderCallback(
                            context, child_items, level + 1))
                    except Exception:
                        output.append(str(v))

            if 'endloop' in self.nodeList:
                output.append(self.nodeList['endloop'].render(context))
            else:
                output.append(self.nodeList['endrecursedict'].render(context))

            context.pop()

        if 'endloop' in self.nodeList:
            output.append(self.nodeList['endrecursedict'].render(context))

        return ''.join(output)

    def render(self, context):
        vals = self.var.resolve(context).items()
        output = self.renderCallback(context, vals, 1)
        return output


@register.tag('recursedict')
def recursedict_tag(parser, token):
    bits = list(token.split_contents())
    if len(bits) != 2 and bits[0] != 'recursedict':
        raise template.TemplateSyntaxError(
            "Invalid tag syntax expected '{% recursedict [dictVar] %}'")
    sequence = parser.compile_filter(bits[1])
    nodeList = {}
    while len(nodeList) < 4:
        nodelist_loop = parser.parse(
            ('value', 'loop', 'endloop', 'endrecursedict'))
        token = parser.next_token()
        nodeList[token.contents] = nodelist_loop
        if token.contents == 'endrecursedict':
            break
    return RecurseDictNode(sequence, nodeList)
