% Stochastic Optimization fitting: SPSA 
format long
record_file = 'western_records.txt';
f_outid = fopen(record_file,'a');
load('carrier2mat.mat')
n=10000;
theta_mat = zeros(size(date,1),15);
for step_index = 1:size(date,1)
    %%i=1;
    y=date(step_index,1);
    q=date(step_index,2);
    %%%fqs = {['AS', 'OO', 'QX', 'UA', 'US', 'VX', 'WN', 'YV'],['AS', 'MQ', 'OO', 'QX', 'UA', 'US', 'VX', 'WN', 'YV'],['AA', 'AS', 'DL', 'MQ', 'OO', 'QX', 'UA', 'US', 'VX', 'WN', 'YV'],['AA', 'AS', 'DL', 'MQ', 'NK', 'OO', 'QX', 'UA', 'US', 'VX', 'WN', 'YV']};
    %%fqs = {[1 1 1 0 0 0 0 1],[1 1 1 1 0 0 0 0 1],[1 1 1 1 1 1 0 0 0 0 1],[1 1 1 1 1 1 1 0 0 0 0 1]};
    %%%fixed_carrier = fqs{q};
    fixed_carrier = ind{step_index};
    %%%PART I: Load scenario carrier and market data
    SESSION_ID = sprintf('western%d_q%d',y,q);
    tic    
    cd('O:\Documents\airline_competition_paper\code\network_games')
    market_data_mat = csvread(sprintf('processed_data/SPSAdatamat_%s.csv',SESSION_ID),1,2);
    %fn_open = strcat('exp_files/carrier_data_basemod_reg1_2_0.0.txt');
    fn_open = strcat(sprintf('processed_data/carrier_data_%s.txt',SESSION_ID));
    outfile_fn = sprintf('exp_files/SPSA_results_fulleq_MAPE_%s',SESSION_ID);
    fid = fopen(fn_open,'r');
    %carrier fixing
    %['AA','AS','MQ','OO','QX','UA','US','WN']
    %%fixed_carrier = [1   0    1    1    1    0    0    0];
    %full 2014
    %['9E', 'AA', 'AS', 'B6', 'CP', 'DL', 'EV', 'F9', 'G7', 'MQ', 'NK', 'OH', 'OO', 'QX', 'RP', 'S5', 'SY', 'UA', 'US', 'VX', 'WN', 'YV', 'YX', 'ZW']
    %fixed_carrier = [1     1     0     0     1     0     1      0     1     1     1     1     1     1     1     1     1    0      0     0    0     1      1     1];
    % west 2014 Q1     AA AS MQ  OO  QX  UA  US  VX WN
    %fixed_carrier = [1  1  1   1   1   0   0   0  0];

    % west 2014 Q3    AS CP DL  OO  QX  UA  US  VX WN   YN
    %%fixed_carrier =  [1  1  1   1   1   0   0   0  0    1];
    %get number of carriers and markets
    tline = fgetl(fid);
    first_line = strsplit(tline);
    num_carriers = str2double(first_line{1});
    num_segments = str2double(first_line{2});
    %get number of competitors in each market
    tline = fgetl(fid);
    second_line = strsplit(tline);
    segment_competitors = eval(second_line{1});
    %get empirical frequencies of each competitor in each market (markets in
    %order of segment_competitors above, competitors within markets in order of
    %market rank)
    tline = fgetl(fid);
    third_line = strsplit(tline);
    empirical_freqs = eval(third_line{1});
    %get carrier index of all frequencies above in same sized array
    tline = fgetl(fid);
    fourth_line = strsplit(tline);
    empirical_carriers = eval(fourth_line{1});
    %get data for each carrier
    tline = fgetl(fid);
    carriers = {};
    line = 1;

    %keep track of which carrier we are on, lock frequencies
    while ischar(tline)
        carrier_cell = strsplit(tline);
        carrier.num = line;
        carrier.A = eval(carrier_cell{1});
        carrier.b = eval(carrier_cell{2})';
        carrier.Markets = eval(carrier_cell{3});
        carrier.freq_inds = eval(carrier_cell{4});
        carrier.coef = eval(carrier_cell{5});
        carrier.freqs = zeros(numel(carrier.Markets),1);
        carrier.profits = zeros(numel(carrier.Markets),1);
        %empirical freqs of carrier in markets sorted
        carrier.emp_freqs = empirical_freqs(empirical_carriers==line);
        carriers{line} = carrier;
        tline = fgetl(fid);
        line = line+1;
    end
    fclose(fid);
    %initialize markets array, constaining frequencies of carriers competing in
    %that market
    Market_freqs = {};
    for i=1:num_segments
        Market_freqs{i}=ones(segment_competitors(i),1);
    end
    %input fixed empirical frequencies into array above, containing empirical market
    %frequencies
    empirical_freq_index = 1;
    for i=1:num_segments
        for j=1:segment_competitors(i)
            %if current carrier as marked by empirical_carrier is marked by
            %used as fixed, set initial frequency in Market_freqs data
            %structure to be the empirical frequency
            if (fixed_carrier(empirical_carriers(empirical_freq_index)) == 1)
                Market_freqs{i}(j)=empirical_freqs(empirical_freq_index);
            end
            %increment empirical frequency index
            empirical_freq_index = empirical_freq_index + 1;
        end
    end

    %vector to include only non fixed carriers in MAPE if desired
    MAPE_incl=zeros(numel(empirical_carriers),1);
    for i=1:numel(MAPE_incl)
        if (fixed_carrier(empirical_carriers(i))==0)
            MAPE_incl(i)=1;
        end
    end
    %MAPE_incl=zeros(numel(empirical_freqs),1);
    %insert_index = 1;
    %for i=1:num_carriers
    %    if (fixed_carrier(i)==0)
    %        MAPE_incl(insert_index:(insert_index+numel(carriers{i}.Markets)-1))=ones(numel(carriers{i}.Markets),1);%%    end
    %    insert_index = insert_index +numel(carriers{i}.Markets);
    %end


    %set optimization parameters

    % good for 2007 west, 2014 full
    % %a=1.9;
    % a=100000; %a=600; %
    % c=20;
    % %c=20;
    % A=100; %A=100;
    % alpha=.602;
    % gamma=.101;
    
    
    %%fclose(fid);
    
    % west 2014
    %a=1.9;
    a=100000; %a=600; %
    c=20;
    %c=20;
    A=100; %A=100;
    alpha=.602;
    gamma=.101;

    %base coefficients
    base_coef=[-150395.5496,-10106.6470,13135.9798,13136.1506,264.4822,-376.1793,-376.1781,270.2080,270.1927,-260.0113,...
        -274960.0,-16470.0,	34936.0,	425.6,	-1300.0,	595.7,...
        -274960.0,-16470.0,	34936.0,	425.6,	-1300.0,	595.7,...
        -95164.0447,-36238.3083,1148.0305];
    %indexes in base coef to insert each of p theta terms
    coef_map = {2,5,[8,9],12,14,16,18,20,22,24,25};%dimension of theta
    %initial theta
    theta = base_coef([2,5,8,12,14,16,18,20,22,24,25])';
    p=numel(theta);
    %order of magnitude scaling
    theta_norm = ones(p,1);
    %%theta_norm = [100,1,1,100,1,1,100,1,1,100,10]';%%theta_norm = ones(p,1);
    %scale theta
    theta = theta./theta_norm;
    %loss function, 1 for MAPE, 2 for negative R^2
    loss_metric  = 1;
    %indices of carriers that have markets to be fixed
    %%fixed_market_carriers = [7,8]; %currently, only WN
    fixed_market_carriers = [];
    %the markets of carriers to be fixed, for each carrier
    %%%fixed_markets = {[],[],[],[],[],[],[2],[1 5 10 16]}; %currently,for WN only LAS_LAX, LAX_OAK, OAK_SAN, LAS_PHX and for US only LAS_PDX
    fixed_markets = {[],[],[],[],[],[],[],[]};
    %LOAD THE MARKETS DATA FILEEEEEEEEE
    %run SPSA
    ys = zeros(2,n);
    best_theta = zeros(numel(theta),1);
    best_loss = 1;
    for k=0:n-1
        %for k=3000:4000-1
        ak=(a/(k+1+A)^alpha)*diag([100,1,1,100,1,1,100,1,1,100,10]);
        ck=c/(k+1)^gamma;
        delta=2*round(rand(p,1))-1;
        thetaplus=theta+ck*delta;
        thetaminus=theta-ck*delta;
        tic
        yplus= network_game_loss_quadratic(thetaplus,theta_norm,coef_map,base_coef,loss_metric,carriers,market_data_mat,fixed_carrier,fixed_market_carriers,fixed_markets,num_carriers,segment_competitors,Market_freqs,empirical_freqs,0,MAPE_incl,outfile_fn);
        yminus=network_game_loss_quadratic(thetaminus,theta_norm,coef_map,base_coef,loss_metric,carriers,market_data_mat,fixed_carrier,fixed_market_carriers,fixed_markets,num_carriers,segment_competitors,Market_freqs,empirical_freqs,0,MAPE_incl,outfile_fn);
        toc
        ys(1,k+1)=yplus;
        ys(2,k+1)=yminus;
        ghat=(yplus-yminus)./(2*ck*delta);
        theta=theta-ak*ghat;
        display(k)
        display(yminus)
        display(yplus)
        if (yminus < best_loss)
            best_loss = yminus;
            best_theta = thetaminus;
        end
        if (yplus < best_loss)
            best_loss = yplus;
            best_theta = thetaplus;
        end

    end
    %quarter 2 theta, test on q3
    %best_theta =  1.0e+04 *    [-0.8468    0.0617    0.0390   -2.1206    0.0986    0.0746   -1.2235    0.1050    0.0517   -2.9842    0.1977]';
    % quarter 4 test on next year
     %1.0e+04 * [   -0.4305    0.0582    0.0098   -1.4157    0.0838    0.0639   -1.6989    0.1313    0.0853   -3.6826    0.2568]';
%best_theta =    1.0e+04 *    [-2.0532,     0.0980,    0.0600,   -1.3927,    0.0584,    0.0707,   -2.2645,    0.1120,    0.0918,   -3.4868,    0.1909]';
    toc
    final_loss=network_game_loss_quadratic(best_theta,theta_norm,coef_map,base_coef,loss_metric,carriers,market_data_mat,fixed_carrier,fixed_market_carriers,fixed_markets,num_carriers,segment_competitors,Market_freqs,empirical_freqs,1,MAPE_incl,outfile_fn);
    toc
    display(final_loss)
    fprintf(f_outid, '%d %d %d %f %f %f %f %f %f %f %f %f %f %f %f \n',[double(y),double(q),double(n),final_loss,best_theta']);
    theta_mat(step_index,:)=[double(y),double(q),double(n),final_loss,best_theta'];
    %%records{i} = {y,q,n,best_loss, best_theta};
    %i=i+1;
    
end

fclose(f_outid);

% plot(diff_ests)
% title('Myopic Best Response Convergence with eps=.1, 6 iterations')
% legend('AS','UA','US','WN')
% xlabel('iterations')
% ylabel('Change in freq. estimate from prev. iteration')
% figure
% plot(diff_ests2)
% title('Myopic Best Response Convergence with eps=.01, 7 iterations')
% legend('AS','UA','US','WN')
% xlabel('iterations')
% ylabel('Change in freq. estimate from prev. iteration')
% M  = zeros(30);
% u = 3:60;
% for i=1:30
%     for j = 1:30
%         M(i,j) = u(i)*46.454 + u(j);
%     end
% end
% 
% 
% 
% %%%%% EXTRA PROCESSING
% %ghat=(ys(1,1)-ys(2,1))./(2*ck*delta);
% Th  = zeros(11,2*11);
% old_yy = yy;
% oldish_yy = yy;
% yy=zeros(1,2*11);
% ind = 1;
% for i=1:11
%     for j=1:2
%         if (j==1)
%             theta_v = new_newbest;
%             theta_v(i)= theta_v(i)+theta_v(i)*.01;
%             y= network_game_loss(theta_v,theta_norm,coef_map,base_coef,loss_metric,carriers,market_data_mat,fixed_carrier,fixed_market_carriers,fixed_markets,num_carriers,segment_competitors,Market_freqs,empirical_freqs,0,MAPE_incl,outfile_fn);
%             yy(ind)=y;
%             ind=ind+1;
%             display(ind)
%         else
%             theta_v = new_newbest;
%             theta_v(i) = theta_v(i)-.01*theta_v(i);
%             y= network_game_loss(theta_v,theta_norm,coef_map,base_coef,loss_metric,carriers,market_data_mat,fixed_carrier,fixed_market_carriers,fixed_markets,num_carriers,segment_competitors,Market_freqs,empirical_freqs,0,MAPE_incl,outfile_fn);
%             yy(ind)=y;
%             ind=ind+1;
%             display(ind)
%         end
%     end
% end
% theta_v = best_theta;
% theta_v(7) = theta_v(7)-.05*theta_v(7);
% y= network_game_loss(theta_v,theta_norm,coef_map,base_coef,loss_metric,carriers,market_data_mat,fixed_carrier,fixed_market_carriers,fixed_markets,num_carriers,segment_competitors,Market_freqs,empirical_freqs,0,MAPE_incl,outfile_fn);
% new_btheta = theta_v;
% 
% 
% theta_v = new_btheta;
% %theta_v(2) = theta_v(2)+.01*theta_v(4);
% %theta_v(4) = theta_v(4)+.01*theta_v(4);
% %theta_v(5) = theta_v(5)-.01*theta_v(5);
% theta_v(8) = theta_v(8)-.05*theta_v(8);
% theta_v(10) = theta_v(10)+.03*theta_v(10);
% %theta_v(4) = theta_v(4)+.02*theta_v(4);
% y= network_game_loss(theta_v,theta_norm,coef_map,base_coef,loss_metric,carriers,market_data_mat,fixed_carrier,fixed_market_carriers,fixed_markets,num_carriers,segment_competitors,Market_freqs,empirical_freqs,0,MAPE_incl,outfile_fn);
% new_newbest=theta_v;
% 
% 
% 
% 
% theta_v=new_newbest;
% theta_v(7) = theta_v(7)-.03*theta_v(7);
% y= network_game_loss(theta_v,theta_norm,coef_map,base_coef,loss_metric,carriers,market_data_mat,fixed_carrier,fixed_market_carriers,fixed_markets,num_carriers,segment_competitors,Market_freqs,empirical_freqs,0,MAPE_incl,outfile_fn);


