import warnings
import os, sys
import time, requests, datetime
import numpy as np
import pandas as pd
import bs4
from bs4 import BeautifulSoup
import seaborn as sns
from matplotlib import pyplot as plt
from PyQt5 import QtGui, QtWidgets


warnings.simplefilter(action='ignore')

FOLDER = '/home/denis/Documents/Covid/Russia/'
PERIOD_DISEASE = 10
DELAY_VACCINATION_DATA = 5


class VaccinationData:
    def __init__(self):
        self.table_name = 'data'
        self.file_name = FOLDER + 'vaccination_data.hdf'
        
        self.quantity_attempts = 3
        self.time_out_after_error = 120
        if os.path.exists(self.file_name):
            self.time_out_after_success = 0
        else:
            self.time_out_after_success = 60
            
        self.domain = 'https://gogov.ru'
        self.start_url = self.domain + '/articles/covid-v-stats'
        self.connection_time_out = 60
        self.headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:68.0) Gecko/20100101 Firefox/68.0'}

    def get(self):
        self._write_vaccination()
        return pd.read_hdf(self.file_name, key=self.table_name)
        
    def _write_vaccination(self):
        if os.path.exists(self.file_name):
            downloaded_regions = pd.read_hdf(self.file_name, key=self.table_name, columns=['Region'])
            downloaded_regions = set(downloaded_regions.Region)
        else:
            downloaded_regions = set()
        
        page = self._download(self.start_url)
        soup = BeautifulSoup(page.text, 'html.parser')
        table = soup.select('#m-table')
        regions = list(table[0].tbody.contents)
        regions = [region for region in regions if type(region)==bs4.Tag]
        regions = sorted(regions, key=lambda region: len(region.a.text), reverse=True) #for correct writing to hdf5
        regions = {region.a.text: region.a.get('href') for region in regions}
        
        for region, url in regions.items():
            if region in downloaded_regions:
                continue
            
            print(region)
            region_page = self._download(self.domain + url)
            region_data = self._get_region_data(region_page)
            region_data['Region'] = region
            region_data.to_hdf(self.file_name, key=self.table_name, mode='a', complevel=4, append=True, format='table')
            
    def _get_region_data(self, page):
        def get_date_int(date_str, prev_date, year):
            pos = date_str.find(' ')
            day = int(date_str[:pos])
            
            current_date = 10000 * year
            if date_str.find('декабря') >= 0:
                current_date += 100 * 12
            elif date_str.find('ноября') >= 0:
                current_date += 100 * 11
            elif date_str.find('октября') >= 0:
                current_date += 100 * 10
            elif date_str.find('сентября') >= 0:
                current_date += 100 * 9
            elif date_str.find('августа') >= 0:
                current_date += 100 * 8
            elif date_str.find('июля') >= 0:
                current_date += 100 * 7
            elif date_str.find('июня') >= 0:
                current_date += 100 * 6
            elif date_str.find('мая') >= 0:
                current_date += 100 * 5
            elif date_str.find('апреля') >= 0:
                current_date += 100 * 4
            elif date_str.find('марта') >= 0:
                current_date += 100 * 3
            elif date_str.find('февраля') >= 0:
                current_date += 100 * 2
            elif date_str.find('января') >= 0:
                current_date += 100 * 1
            else:
                raise RuntimeError("Couldn't recognize the date: " + date_str + ' !')
            
            current_date += day
            if current_date > prev_date:
                #year has changed
                current_date -= 10000
                year -= 1
            return current_date, year
                
        def get_int(int_str):
            int_str = int_str.replace(' ', '')
            if int_str:
                return np.uint32(int_str)
            else:
                return 0
            
        soup = BeautifulSoup(page.text, 'html.parser')
        
        try:
            table = soup.select('.table-box-400 > table:nth-child(1)')
            table_data = list(table[0].contents)
        except:
            raise RuntimeError("Page doesn't contain data table!")
        
        table_data = [row for row in table_data if type(row)==bs4.Tag]
        columns = table_data[0].text
        if columns.find('ревакцинация') >= 0:
            exist_revaccination = True
        else:
            exist_revaccination = False
        
        current_date = datetime.date.today()
        prev_date = int(current_date.strftime('%Y%m%d'))
        
        data = []
        year = current_date.year
        rows = table_data[1:]
        for row in rows:
            values = row.find_all('td')
            current_date_str = values[0].text
            current_date_int, year = get_date_int(current_date_str, prev_date, year)
            prev_date = current_date_int
                
            vaccinated = get_int(values[1].text)
            fully_vaccinated = get_int(values[2].text)
            
            if exist_revaccination:
                revaccinated = get_int(values[3].text)
            else:
                revaccinated = 0
                
            data.append((current_date_int, vaccinated, fully_vaccinated, revaccinated))
            
        region_data = pd.DataFrame(data, columns=['Date', 'Vaccinated', 'FullyVaccinated', 'Revaccinated'])
        region_data = self._clear_duplicated_values(region_data)
        region_data = self._delete_empty_rows(region_data)
        return region_data.astype({'Date':'int32', 'Vaccinated':'uint32', 'FullyVaccinated':'uint32', 'Revaccinated':'uint32'})
    
    def _clear_duplicated_values(self, region_data):
        for column in ('Vaccinated', 'FullyVaccinated', 'Revaccinated'):
            prev_value = None
            for index in reversed(region_data.index):
                value = region_data.loc[index][column]
                if prev_value is not None and value == prev_value:
                    region_data[column].loc[index] = 0
                prev_value = value
        return region_data
    
    def _delete_empty_rows(self, region_data):
        empty_rows = []
        for index in region_data.index:
            row = region_data.loc[index]
            if row.Vaccinated + row.FullyVaccinated + row.Revaccinated == 0:
                empty_rows.append(index)
        region_data = region_data.drop(empty_rows, axis=0) 
        return region_data.reset_index(drop=True)       
        
    def _download(self, url): 
        for _ in range(self.quantity_attempts):
            try:
                data = requests.get(url, timeout=self.connection_time_out, headers=self.headers)
                if data.status_code == 200:
                    time.sleep(self.time_out_after_success)
                    break
                else:
                    time.sleep(self.time_out_after_error)
                    continue
            except:
                time.sleep(self.time_out_after_error)
        else:
            raise RuntimeError('Failed to load page:' + url)
        
        return data
         
         
class MortalityReader:
    def __init__(self, month_date):
        self.month_date = month_date
        self.file_name = self._get_file_name()
        self.death_significance_limit = 1000  

    def read(self):
        names = ['Region', 'Mortality_2021', 'Mortality_2020']
        data = pd.read_csv(self.file_name, sep=';', header=0, names=names, usecols=[0, 5, 6])
        
        data = self._delete_bad_rows(data)
        data['Date'] = self.month_date
        data['K_mortality'] = data.Mortality_2021 / data.Mortality_2020
        return data.drop(['Mortality_2021', 'Mortality_2020'], axis=1)
    
    def _get_file_name(self): 
        month = self.month_date.month
        year = self.month_date.year
        
        if month < 10:
            month_str = '0' + str(month)
        else:
            month_str = str(month)
            
        file_name = FOLDER + 'edn' + month_str + '_' + str(year) + 'ut.csv'
        if os.path.exists(file_name):
            return file_name
        
        file_name = FOLDER + 'edn' + month_str + '_' + str(year) + '.csv'
        if os.path.exists(file_name):
            return file_name
        
        raise RuntimeError("The file:"  + file_name + " does not exist!")
           
    def _delete_bad_rows(self, data):   
        data = data[data.Mortality_2020 > self.death_significance_limit]
        
        bad_rows = []
        for index in data.index:
            row = data.loc[index]
            region = row.Region
            if region.startswith(' '):
                bad_rows.append(index)
                
            elif region.find('без автономии') >= 0:
                bad_rows.append(index)
                
        return data.drop(bad_rows, axis=0)


class Analyzator:
    def __init__(self, analyzed_months):
        self.analyzed_months = analyzed_months
        self.population = self._get_population()
        
        downloader = VaccinationData()
        self.vaccination = downloader.get()
        self.vaccination = self.vaccination.reset_index(drop=True)
        
        self.plot_density_of_corrections = False
        self.statistic_corrections = []
        self.permissible_interpolate_correction = 0.1
        self.permissible_extrapolate_correction = 0.05
        
    def analyze(self):
        for month in self.analyzed_months:
            reader = MortalityReader(month)
            mortality = reader.read()
            
            fig = VaccinationFigure(month, nrows=2, ncolumns=1)
            for indicator in ('Vaccinated', 'FullyVaccinated'):
                month_vaccination = self._get_vaccination_for_month(month, indicator)
                all_data = self._merge_all_dataframes(mortality, month_vaccination,  indicator)
            
                x = all_data.PartUnvaccined
                y = all_data.K_mortality
                fig.add_scatter(x, y, indicator)
        
        self._plot_density_of_corrections()
        plt.show()

    def _plot_density_of_corrections(self):
        if not self.plot_density_of_corrections:
            return
        
        fig = plt.figure()
        fig.suptitle('Density corrections of value', fontsize=14)
        
        ax = fig.add_subplot(1, 1, 1)
        ax.set_xlabel('Part of correction', fontsize=12)
        ax.set_ylabel('Density of probability', fontsize=12)

        corrections = pd.Series(self.statistic_corrections)
        corrections = corrections[corrections < 1]
        sns.distplot(corrections, bins=100, ax=ax, color='k')
        
    def _get_population(self):
        file_name = FOLDER + 'Popul2021_Site-1.csv'
        return pd.read_csv(file_name, sep=';', header=0, names=['Region', 'Population'], usecols=(0,1))
      
    def _get_vaccination_for_month(self, month, column):
        def get_date(date_int):
                date_str = str(date_int)
                date_str = date_str[:4] + '-' + date_str[4:6] + '-' + date_str[6:]
                return np.datetime64(date_str, 'D')
        
        shift = DELAY_VACCINATION_DATA - PERIOD_DISEASE
        border_date = month + datetime.timedelta(days=shift)
        border_date = int(border_date.strftime('%Y%m%d'))
        
        vaccination = self.vaccination    
        vaccination_for_month = {}
        regions = set(self.vaccination.Region)
        for region in regions:
            region_data = vaccination[(vaccination.Region==region) & (vaccination[column] > 0)]
            region_data = region_data.set_index('Date')
            if not region_data.index.is_monotonic_decreasing:
                region_data.sort_index(ascending=False)
            
            start_row = region_data.iloc[0]
            last_row = region_data.iloc[-1]
            if border_date > start_row.name:
                first_row = start_row
                second_row = region_data.iloc[1]
                
            elif border_date < last_row.name:
                first_row = region_data.iloc[-2]
                second_row = last_row
                
            else:
                first_pos = region_data.index.get_loc(border_date, method='ffill')
                second_pos = region_data.index.get_loc(border_date, method='bfill')
                first_row = region_data.iloc[first_pos]
                second_row = region_data.iloc[second_pos]
            
            derivative_period = np.busday_count(get_date(second_row.name), get_date(first_row.name))
            shift_period = np.busday_count(get_date(second_row.name), get_date(border_date))
            if derivative_period == 0 or shift_period == 0:
                value = second_row[column]
                
            else:
                difference = first_row[column] - second_row[column]
                derivative = difference / derivative_period
                value = second_row[column] + derivative * shift_period
                
                #positive value mean interpolation, negative- extrapolation
                delta_first = first_row[column] - value 
                delta_second = value - second_row[column]
                if abs(delta_first) < abs(delta_second):
                    part = delta_first / first_row[column]
                else:
                    part = delta_second / second_row[column]
                    
                if self.plot_density_of_corrections:
                    self.statistic_corrections.append(part)
                    
                if part > self.permissible_interpolate_correction or -part > self.permissible_extrapolate_correction:
                    #too big corrections
                    continue
                
            if value < 0:
                value = 0
            
            vaccination_for_month[region] = np.uint32(value)
        return vaccination_for_month
    
    def _merge_all_dataframes(self, mortality, month_vaccination,  column):
        def replace_regions(df):
            for index in df.index:
                row = df.loc[index]
                
                region = row.Region
                pos = region.find('(')
                if pos >= 0:
                    region = region[:pos]
                    
                region = region.replace('автономный округ', '')
                region = region.replace('автономная область', '')
                region = region.replace('авт.округ', '')
                region = region.replace('авт.область', '')
                region = region.replace('Республика', '')
                region = region.replace('область', '')
                region = region.replace('край', '')
                region = region.replace('обл.', '')
                region = region.replace('АО', '')
                region = region.replace('г.', '')
                region = region.strip()
                
                if region == 'Чувашская':
                    region = 'Чувашия'
                    
                elif region == 'Ханты-Мансийский -Югра':
                    region = 'Ханты-Мансийский'
                    
                elif region == 'Удмуртская':
                    region = 'Удмуртия'
                    
                elif region == 'Hенецкий':
                    region = 'Ненецкий'
                    
                elif region == 'Ямало-Hенецкий':
                    region = 'Ямало-Ненецкий'
                    
                elif region == 'Hижегородская':
                    region = 'Нижегородская'
                    
                df['Region'].loc[index] = region
            return df
        
        population = replace_regions(self.population)
        mortality = replace_regions(mortality)  
          
        vaccination = pd.DataFrame({'Region': month_vaccination.keys(), column: month_vaccination.values()})
        vaccination = replace_regions(vaccination)
        
        all_data = pd.merge(mortality, population, on='Region')
        all_data = pd.merge(all_data, vaccination, on='Region')
        
        all_data['PartUnvaccined'] = 1 - all_data[column] / all_data.Population
        all_data = all_data[(all_data.PartUnvaccined<1) & (all_data.K_mortality>0)]
        all_data = all_data.drop(['Date', 'Region', column, 'Population'], axis=1)
        return all_data
    
    
class VaccinationFigure:
    def __init__(self, month_date, nrows, ncolumns):
        self.month_date = month_date
        self.nrows = nrows
        self.ncolumns = ncolumns
        self.number_picture = 1
        
        self.fig = plt.figure()
        self.fig.suptitle(self._title(), fontsize=14)
        sns.set_style('darkgrid')
        
    def add_scatter(self, x, y, indicator):
        ax = self.fig.add_subplot(self.nrows, self.ncolumns, self.number_picture)
        self.number_picture += 1
        
        # ax.scatter(x, y)
        sns.regplot(x, y)
        ax.set_xlabel(self._xlabel(indicator), fontsize=12)
        ax.set_ylabel(self._ylabel(indicator), fontsize=12)
        
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

    def _title(self):
        month = self.month_date.month
        if month == 10:
            month_name = 'Октябрь'
        elif month == 11:
            month_name = 'Ноябрь'
        elif month == 12:
            month_name = 'Декабрь'
            
        return month_name + ' 2021г'

    def _xlabel(self, indicator):
        if indicator == 'Vaccinated':
            return 'Доля НЕ вакцинированного населения'
        elif indicator == 'FullyVaccinated':
            return 'Доля НЕполностью вакцинированного населения'
    
    def _ylabel(self, indicator):
        return 'Избыточная смертность'


def analyze():
    analyzed_months = [datetime.date(2021, month, 1) for month in (10, 11, 12)]
    analyzotor = Analyzator(analyzed_months)
    analyzotor.analyze()  
    print('Operation successfully completed!')
        
def build_window():       
    window = QtWidgets.QWidget()
    window.resize(600, 100)
    
    label = QtWidgets.QLabel()
    label.setText('Анализ избыточной смертности в зависимости от доли невакцинированного населения в России')

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
    
        