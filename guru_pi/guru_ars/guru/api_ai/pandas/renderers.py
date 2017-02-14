import os

import pandas_highcharts.core
import pandas as pd
from django.conf import settings
from django.template import Template, Context
from django.template import loader

class RenderTemplate():

    def __init__(self, path):
        templates_base_dir = os.path.join(settings.BASE_DIR, 'guru/templates')
        self.template = Template(open(os.path.join(templates_base_dir, path), 'rt').read())
        self.js = ''

    def render_to(self, section, content):
        self.js += 'document.getElementById("section_%s_header").innerHTML = "%s";'%(section, content['header'])
        #self.js += '$("#section_%s_header").html("%s");'%(section, content['header'])

        if content['type'] == 'graph':
            chart = content['body']
            kind = content.get('graph_type', 'line')
            title = content.get('title', 'Report')
            chart = chart.unstack(level=0)
            chart = pandas_highcharts.core.serialize(chart, title=title, render_to='section_%s_body'%(section), output_type='dict', kind=kind, figsize=(928, 520))
            chart['chart']['backgroundColor'] = "#2a2a2a"

            chart['colors'] = [ "#5290e9", "#71b37c", "#ec932f", "#e14d57", "#965994", "#9d7952", "#cc527b", "#33867f", "#ada434" ]
            #chart['tooltip'] = {'crosshairs': True, 'shared': True},
            chart['plotOptions'] = {'spline': {'marker': {'radius':4, 'lineColor': '#666666', 'lineWidth': 1}}}

            chart = pd.io.json.dumps(chart)

            print(str(chart))

            self.js += 'new Highcharts.Chart(%s);'%(str(chart))
        else:#table or anything else
            self.js += """document.getElementById("section_%s_body").innerHTML = '%s';"""%(section, content['body'])
            #self.js += '$("#section_%s_body").html("%s");'%(section, content['body'])

    def render(self):
        context = Context({'js': self.js})
        return self.template.render(context)



class GenerateHtml():
    #generate html report from table/chart/text data.
    def __init__(self, save_in='htmlcode.html'):
        self.template_base_dir = os.path.join(settings.BASE_DIR, 'guru/templates/report_templates')
        self.template_master = Template(open(os.path.join(self.template_base_dir, 'master.html'), 'r').read())
        self.template_message = Template(open(os.path.join(self.template_base_dir, 'message.html'), 'r').read())
        self.template_table = Template(open(os.path.join(self.template_base_dir, 'table.html'), 'r').read())
        self.template_chart = Template(open(os.path.join(self.template_base_dir, 'chart.html'), 'r').read())
        self.o_file = open(os.path.join(self.template_base_dir, save_in), 'w')
        self.final_text = ''

    def append(self, type, data):
        if type == 'message':
            context = Context({'DATA': data})
            self.final_text += self.template_message.render(context)

        elif type == 'table':
            data = loader.get_template(os.path.join(self.template_base_dir, 'create_table.html')).render(data)
            context = Context({'DATA': data})
            self.final_text += self.template_table.render(context)

        elif type == 'chart':
            context = Context({'DATA': data})
            self.final_text += self.template_chart.render(context)

    def generate(self):
        context = Context({'CONTENT': self.final_text})
        _data = self.template_master.render(context)
        self.o_file.write(_data)
        self.o_file.close()


