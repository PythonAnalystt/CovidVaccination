import warnings
import sys
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from PyQt5 import QtGui, QtWidgets
from enum import Enum


warnings.simplefilter(action='ignore')

FOLDER = '/home/denis/Documents/Covid/USA/'
PERIOD_DISEASE = 10
DELAY_VACCINATION_DATA = 5
USE_PREDICTED_MORTALITY = True


class AgeGroups(Enum):
    all     = 'All ages'
    above65 = 'Above 65 years'
    under65 = 'Under 65 years'
    
    
class Analizator:
    def __init__(self, months):
        self.months = months
        
    def analyze(self):
        vaccination = self._read_vaccination()
        vaccination = self._count_unvaccination(vaccination)
        
        mortality = self._read_mortality()
        
        for month in self.months:
            days = PERIOD_DISEASE-DELAY_VACCINATION_DATA
            border_date = month - np.timedelta64(days, 'D')
            month_vaccination = vaccination[vaccination.Date==border_date]
            
            figure = VaccinationFigure(month)
            for age_group in AgeGroups:
                current_mortality = self._select_mortlality(mortality, month, age_group)
        
                previous_year_month = self._add_months(month, -12)
                previous_mortality = self._select_mortlality(mortality, previous_year_month, age_group)
        
                suffixes=('_current_year', '_previous_year')
                all_mortality = pd.merge(current_mortality, previous_mortality, on='State', suffixes=suffixes)
                all_mortality['K_Mortality'] = all_mortality.Deaths_current_year / all_mortality.Deaths_previous_year
                all_mortality = all_mortality.drop(['Deaths_current_year', 'Deaths_previous_year'], axis=1)
                all_mortality = all_mortality[all_mortality.K_Mortality>0.3]
                
                data = pd.merge(month_vaccination, all_mortality, on='State')
                
                if age_group == AgeGroups.all:
                    x1 = data.FirstPart
                    x2 = data.SecondPart
                elif age_group == AgeGroups.above65:
                    x1 = data.First65PlusPart
                    x2 = data.Second65PlusPart
                else:
                    x1 = data.First65MinusPart
                    x2 = data.Second65MinusPart
                
                y1 = data.K_Mortality
                y2 = data.K_Mortality
                    
                figure.add_scatters(x1, y1, x2, y2)
        
        plt.show()
                
    def _read_vaccination(self):
        file_name = FOLDER + 'COVID-19_Vaccinations_in_the_United_States_Jurisdiction.csv'
        names = ['Date', 'State', 'First', 'FirstPct', 'First65Plus', 'First65PlusPct', 'Second', 'SecondPct', 'Second65Plus', 'Second65PlusPct']
        usecols = (0, 2, 25, 26, 31, 32, 33, 34, 39, 40)
        data = pd.read_csv(file_name, header=0, names=names, usecols=usecols, parse_dates=[0])
        return data.astype({'Date': 'datetime64[D]'})
    
    def _count_unvaccination(self, vaccination):
        vaccination['Population']        = vaccination.First * 100 / vaccination.FirstPct
        vaccination['Population65Plus']  = vaccination.First65Plus * 100 / vaccination.First65PlusPct
        vaccination['Population65Minus'] = vaccination.Population - vaccination.Population65Plus
        
        vaccination['First65Minus']     = vaccination.First - vaccination.First65Plus
        vaccination['First65MinusPart'] = 1 - vaccination.First65Minus /  vaccination.Population65Minus
        vaccination['FirstPart']        = 1 - vaccination.FirstPct / 100
        vaccination['First65PlusPart']  = 1 - vaccination.First65PlusPct / 100
        
        vaccination['Second65Minus']     = vaccination.Second - vaccination.Second65Plus
        vaccination['Second65MinusPart'] = 1 - vaccination.Second65Minus /  vaccination.Population65Minus
        vaccination['SecondPart']        = 1 - vaccination.SecondPct / 100
        vaccination['Second65PlusPart']  = 1 - vaccination.Second65PlusPct / 100
        
        removed_columns = ['First', 'FirstPct', 'First65Plus', 'First65PlusPct', 'Second', 'SecondPct', 'Second65Plus', 'Second65PlusPct', \
                           'Population', 'Population65Plus', 'Population65Minus', 'First65Minus', 'Second65Minus']
        return vaccination.drop(removed_columns, axis=1)
        
    def _read_mortality(self):
        file_name = FOLDER + 'Weekly_Counts_of_Deaths_by_Jurisdiction_and_Age.csv'
        names = ['EndWeek', 'State', 'Above65', 'Deaths', 'TypeProcessing']
        usecols = (1, 2, 5, 6, 8)
        converters = {'Above65': lambda value: True if value in ('65-74 years', '75-84 years', '85 years and older') else False}
        data = pd.read_csv(file_name, header=0, names=names, usecols=usecols, converters=converters, parse_dates=[1])
        
        if USE_PREDICTED_MORTALITY:
            data = data[data.TypeProcessing == 'Predicted (weighted)']
        else:
            data = data[data.TypeProcessing == 'Unweighted']
            
        data = data.drop('TypeProcessing', axis=1)
        
        data = data.astype({'EndWeek': 'datetime64[D]'})
        data['StartWeek'] = data.apply(lambda row: row.EndWeek - np.timedelta64(6, 'D'), axis=1)
        return data
    
    def _select_mortlality(self, mortality, month, age_group):
        if age_group == AgeGroups.all:
            mask_age = [True] * len(mortality)
        elif age_group == AgeGroups.above65:
            mask_age = mortality.Above65==True
        elif age_group == AgeGroups.under65:
            mask_age = mortality.Above65==False
        else:
            raise RuntimeError('Wrong age group!')
        
        next_month = self._add_months(month, 1)
        mortality = mortality[(mortality.EndWeek>=month) & (mortality.StartWeek<next_month) & (mask_age)]
        mortality = mortality.drop('Above65', axis=1)
        
        #correct uncomplete weeks
        day = np.timedelta64(1, 'D')
        week = np.timedelta64(7, 'D')
        for index in mortality.index:
            row = mortality.loc[index]
            if row.StartWeek < month:
                K = (row.EndWeek - month + day) / week 
                mortality.loc[index, 'Deaths'] = np.round(K * row.Deaths)
                
            elif row.EndWeek >= next_month:
                K = (next_month - row.StartWeek) / week
                mortality.loc[index, 'Deaths'] = np.round(K * row.Deaths)
        
        grouped = mortality.groupby('State')
        mortality = grouped.sum()
        mortality = mortality.reset_index()
        return mortality  
    
    def _add_months(self, current_month, quantity):
        year_int = current_month.astype(object).year
        month_int = current_month.astype(object).month
        
        year_int += quantity // 12
        month_int += quantity % 12
        
        if month_int <= 0:
            year_int -= 1
            month_int += 12
            
        elif month_int > 12:
            year_int += 1
            month_int -= 12
        
        if month_int < 10:
            month_str = '0' + str(month_int)    
        else:
            month_str = str(month_int)
        
        year_str = str(year_int)
        return np.datetime64(year_str + '-' + month_str, 'D')        
            
            
class VaccinationFigure:
    def __init__(self, month_date):
        self.month_date = month_date.astype(object) #convert to Python datetime
        self.nrows = 3
        self.ncolumns = 2
        self.number_row = 0
        
        self.fig = plt.figure()
        self.fig.suptitle(self._title(), fontsize=14)
        sns.set_style('darkgrid')
        
    def add_scatters(self, x1, y1, x2, y2):
        for number_column in range(2):
            if number_column == 0:
                x = x1
                y = y1
            else:
                x = x2
                y = y2
            
            number_picture = 2 * self.number_row + number_column + 1 
            ax = self.fig.add_subplot(self.nrows, self.ncolumns, number_picture)
            
            sns.regplot(x, y)
            ax.set_xlabel(self._xlabel(number_column), fontsize=10)
            ax.set_ylabel(self._ylabel(number_column), fontsize=10)
            
            #plot trendline    
            z = np.polyfit(x, y, 1)
            p = np.poly1d(z)
            ax.plot(x, p(x), "r-")
            
            x_text = np.min(x)
            y_text = 1.025 * np.mean(y)
            equation_text = 'y = ' + str(round(z[0], 2)) + 'x '
            equation_text += '+' if z[1]>0 else ''
            equation_text += str(round(z[1], 2))
            ax.text(x_text, y_text, equation_text, fontsize=10)
        self.number_row += 1

    def _title(self):
        month = self.month_date.month
        if month == 10:
            month_name = 'Октябрь'
        elif month == 11:
            month_name = 'Ноябрь'
        elif month == 12:
            month_name = 'Декабрь'
            
        return month_name + ' 2021г, USA'

    def _xlabel(self, number_column):
        if number_column == 0:
            xlabel = 'Доля НЕ вакцинированного населения'
        elif number_column == 1:
            xlabel = 'Доля НЕполностью вакцинированного населения'
        else:
            raise RuntimeError('Wrong number_column!')
        
        if self.number_row == 0:
            xlabel += ', all ages'
        elif self.number_row == 1:
            xlabel += ', above 65 years'
        elif self.number_row == 2:
            xlabel += ', under 65 years'
        else:
            raise RuntimeError('Wrong number column!')
        
        return xlabel
    
    def _ylabel(self, number_column):
        if number_column == 0:
            return 'Избыточная смертность'
        else:
            return ''

    
def analyze():
    analyzed_months = [np.datetime64('2021-'+ month, 'D') for month in ('10', '11', '12')]
    analyzotor = Analizator(analyzed_months)
    analyzotor.analyze()  
    print('Operation successfully completed!')
        
def build_window():       
    window = QtWidgets.QWidget()
    window.resize(600, 100)
    
    label = QtWidgets.QLabel()
    label.setText('Анализ избыточной смертности в зависимости от доли невакцинированного населения в США')

    myFont=QtGui.QFont()
    myFont.setPointSize(14)
    myFont.setBold(True)
    label.setFont(myFont)

    button = QtWidgets.QCommandLinkButton("Analyze")
    button.clicked.connect(analyze)
    
    vbox = QtWidgets.QVBoxLayout()
    vbox.addWidget(label)
    vbox.addWidget(button)
    window.setLayout(vbox)
    return window
    
if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    window = build_window()    
    window.show()
    sys.exit(app.exec_())    
