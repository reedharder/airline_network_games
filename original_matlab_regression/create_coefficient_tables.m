%num players
n=2;
%directory
datadir = 'C:/users/d29905P/Documents/airline_competition_paper/';
%profit function
airline_profit_func = @profit_nplayer_scheddelay;
%number of regression coefficients
if (n ==1)
    num_coef = 3;
else
    num_coef = 1+2*n+nchoosek(n,2);
end

%define defaults
m_def = .456;
phi_def = 5.1;
beta_def = -.021; %as per hansen and liu, perhaps -.0045 as per original model
N_def=0.5;
S_def = 125;

%vary m
param_vec = .1:.1:1;
datatable_m = zeros(numel(param_vec),num_coefs + 7);
for i =1:numel(param_vec);
    m = param_vec(i);
    [coefs, r2, r2adj, mdl,ps,fvals] = second_stage_nplayer(airline_profit_func,n,phi_def,m,beta_def,S_def,N_def);
    display(sprintf('with m=%d, rquared: %f, rsquared adj: %f',m,r2,r2adj))
    display(coefs)
    datatable_m(i,:) = [phi_def,m,beta_def,S_def,N_def,coefs,r2,r2adj];
end
dlmwrite(sprintf('%scoef_vary_m_%dplayer.csv',datadir,n),datatable_m,'delimiter',',','precision','%.4f')

%vary phi
param_vec = 1:10;
datatable_phi = zeros(numel(param_vec),num_coefs + 7);
for i = 1:numel(param_vec)
    phi = param_vec(i);
    [coefs, r2, r2adj, mdl,ps,fvals] = second_stage_nplayer(airline_profit_func,n,phi,m_def,beta_def,S_def,N_def);
    display(sprintf('with phi=%d, rquared: %f, rsquared adj: %f',phi,r2,r2adj))
    display(coefs)
    datatable_phi(i,:) = [phi,m_def,beta_def,S_def,N_def,coefs,r2,r2adj];
end
dlmwrite(sprintf('%scoef_vary_phi_%dplayer.csv',datadir,n),datatable_phi,'delimiter',',','precision','%.4f')

%vary beta
param_vec = 0.001:001:0.25;
datatable_beta = zeros(numel(param_vec),num_coefs + 7);
for i = 1:numel(param_vec)
    beta = param_vec(i);
    [coefs, r2, r2adj, mdl,ps,fvals] = second_stage_nplayer(airline_profit_func,n,phi_def,m_def,beta,S_def,N_def);
    display(sprintf('with beta=%d, rquared: %f, rsquared adj: %f',m,r2,r2adj))
    display(coefs)
    datatable_beta(i,:) = [phi_def,m_def,beta,S_def,N_def,coefs,r2,r2adj];
end
dlmwrite(sprintf('%scoef_vary_beta_%dplayer.csv',datadir,n),datatable_beta,'delimiter',',','precision','%.4f')

%vary N
param_vec = 0:.1:1;
datatable_N = zeros(numel(param_vec),num_coefs + 7);
for i = 1:numel(param_vec)
    N = param_vec(i);
    [coefs, r2, r2adj, mdl,ps,fvals] = second_stage_nplayer(airline_profit_func,n,phi_def,m_def,beta_def,S_def,N);
    display(sprintf('with N=%d, rquared: %f, rsquared adj: %f',m,r2,r2adj))
    display(coefs)
    datatable_N(i,:) = [phi_def,m_def,beta_def,S_def,N,coefs,r2,r2adj];
end
dlmwrite(sprintf('%scoef_vary_N_%dplayer.csv',datadir,n),datatable_N,'delimiter',',','precision','%.4f')

%vary S
param_vec = [10000,1000,250,225,200,175,150,125,100,75,50,25];
datatable_S = zeros(numel(param_vec),num_coefs + 7);
for i = 1:numel(param_vec)
    N = param_vec(i);
    [coefs, r2, r2adj, mdl,ps,fvals] = second_stage_nplayer(airline_profit_func,n,phi_def,m_def,beta_def,S_def,N);
    display(sprintf('with N=%d, rquared: %f, rsquared adj: %f',m,r2,r2adj))
    display(coefs)
    datatable_S(i,:) = [phi_def,m_def,beta_def,S_def,N,coefs,r2,r2adj];
end
dlmwrite(sprintf('%scoef_vary_beta_%dplayer.csv',datadir,n),datatable_beta,'delimiter',',','precision','%.4f')
