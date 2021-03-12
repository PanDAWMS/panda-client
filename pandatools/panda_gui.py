import pandas as pd
import plotly.express as px
from jupyter_dash import JupyterDash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import plotly.graph_objects as go
from dash.dependencies import Input, Output

from . import queryPandaMonUtils


def show_task(jeditaskid, verbose=False, mode='inline'):
    # get task
    task = queryPandaMonUtils.query_tasks(23518002, verbose=False)[-1][0]
    # get tasks of the user
    tasks = queryPandaMonUtils.query_tasks(username=task['username'], verbose=False)[-1]
    tids = set([x['jeditaskid'] for x in tasks])
    tids.add(jeditaskid)

    # Build App
    app = JupyterDash(__name__)
    app.layout = html.Div([
        html.Div([
            html.H2("TaskID: "),
            dcc.Dropdown(
                id='dropdown_taskid',
                options=[{'label': i, 'value': i} for i in tids],
                value=jeditaskid
            ),],
            style={'display': 'inline-block', 'width': '20%'}
        ),
        html.Div([
            html.Div([
                html.H2('Task Attributes'),
                dash_table.DataTable(id='00_table',
                                     columns=[{'id': 'attribute', 'name': 'attribute'},
                                              {'id': 'value', 'name': 'value'}],
                                     page_action='none',
                                     style_table={'height': '330px', 'overflowY': 'auto'},
                                     style_cell_conditional=[
                                         {
                                             'if': {'column_id': 'value'},
                                             'textAlign': 'left'
                                         },
                                     ]),],
                style={'display': 'inline-block', 'width': '49%', 'float': 'left', 'padding-top': '30px'}
            ),
            html.Div([
                dcc.Graph(id='01_graph'),],
                style={'display': 'inline-block', 'width': '49%'}
            ),
        ],),
        html.Div([
            html.Div([
                dcc.Graph(id='10_graph')],
                style={'display': 'inline-block', 'width': '49%'}),
            html.Div([
                dcc.Graph(id='11_graph')],
                style={'display': 'inline-block', 'width': '49%'})
        ],),
    ])

    # Run app and display result inline in the notebook
    app.run_server(mode=mode)


    @app.callback(
        Output('00_table', 'data'),
        Output('01_graph', 'figure'),
        Output('10_graph', 'figure'),
        Output('11_graph', 'figure'),
        Input('dropdown_taskid', "value")
    )
    def make_elements(jeditaskid):
        verbose = False
        task = queryPandaMonUtils.query_tasks(jeditaskid, verbose=verbose)[-1][0]
        jobs = queryPandaMonUtils.query_jobs(jeditaskid, drop=False, verbose=verbose)[-1]['jobs']
        jobs = pd.DataFrame(jobs)

        # task data
        task_data = [{'attribute': k, 'value': task[k]} for k in task if isinstance(task[k], (str, type(None)))]

        # figures
        site_fig = px.histogram(jobs, x="computingsite", color="jobstatus")
        ram_fig = px.histogram(jobs, x="maxrss")

        exectime_fig = go.Figure()
        legend_set = set()
        for d in jobs.itertuples(index=False):
            if d.jobstatus == 'finished':
                t_color = 'green'
            elif d.jobstatus == 'failed':
                t_color = 'red'
            else:
                t_color = 'orange'
            if d.jobstatus not in legend_set:
                show_legend = True
                legend_set.add(d.jobstatus)
                exectime_fig.add_trace(
                    go.Scatter(
                        x=[d.creationtime, d.creationtime],
                        y=[d.pandaid, d.pandaid],
                        mode="lines",
                        line=go.scatter.Line(color=t_color),
                        showlegend=True,
                        legendgroup=d.jobstatus,
                        name=d.jobstatus,
                        hoverinfo='skip'
                    )
                )
            exectime_fig.add_trace(
                go.Scatter(
                    x=[d.creationtime, d.endtime],
                    y=[d.pandaid, d.pandaid],
                    mode="lines",
                    line=go.scatter.Line(color=t_color),
                    showlegend=False,
                    legendgroup=d.jobstatus,
                    name="",
                    hovertemplate="PandaID: %{y:d}")
            )
        exectime_fig.update_xaxes(range=[jobs['creationtime'].min(), jobs['endtime'].max()],
                                  title_text='Job Lifetime')
        exectime_fig.update_yaxes(range=[jobs['pandaid'].min() * 0.999, jobs['pandaid'].max() * 1.001],
                                  title_text='PandaID')

        return task_data, site_fig, ram_fig, exectime_fig
