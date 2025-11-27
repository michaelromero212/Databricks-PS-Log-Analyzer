import React from 'react';
import Plot from 'react-plotly.js';

const PlotlyChart = ({ data, layout }) => {
    return (
        <div className="w-full h-full min-h-[300px]">
            <Plot
                data={data}
                layout={{
                    ...layout,
                    autosize: true,
                    margin: { l: 40, r: 20, t: 40, b: 40 },
                    font: { family: 'Inter, sans-serif' },
                    paper_bgcolor: 'rgba(0,0,0,0)',
                    plot_bgcolor: 'rgba(0,0,0,0)',
                }}
                useResizeHandler={true}
                style={{ width: '100%', height: '100%' }}
                config={{ displayModeBar: false }}
            />
        </div>
    );
};

export default PlotlyChart;
