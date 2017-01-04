import os

import pandas_highcharts.core
import pandas as pd
from django.conf import settings
from django.template import Template, Context

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

