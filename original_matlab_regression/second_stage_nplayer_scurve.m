function [coefs, r2, r2adj, mdl,ps,fvals] = second_stage_nplayer_scurve(airline_profit_func,n,alpha,beta,S,N)
options = optimset('Display', 'off') ;
tic
ps=zeros(20^n,5+2*n);
fvals=zeros(20^n,5+2*n);
%symmetric S, for now
S_vec=ones(n,1)*S;
M=1000; C=ones(n,1)*10000;
p0=100;
eps=0.1;
c=0;
count=0;
%solve second stage game for all frequency combinations
fs  = permn(1:20,2);
tic
for combo = 1:20^n
    
    f = fs(combo,:)';
    p=ones(n,1)*p0;
    diffs=ones(n,1)*(eps+1);
    fval=zeros(n,1);
    while sum(diffs)>eps
        for i=1:n
            p_i=p(i);            
            fun_i=@(p_i)airline_profit_func(p,f,M,S_vec,C,alpha,beta,N,i,p_i); 
            [x_i, fval(i)]=fmincon(fun_i,p0,[],[],[],[],0,[],[],options);
            diffs(i)=abs(p(i)-x_i);
            p(i)=x_i;            
        end
        c=c+1;
    end
    count=count+1;
    ps(count,:)=[alpha,beta,S_vec',N,f',p'];
    fvals(count,:)=[alpha,beta,S_vec',N,f',-fval'];
    if (mod(count,1000)==0)
        display(count)
        toc
    end            
end
toc
%generate model
if (n==1)
    X=[fs(:,1),fs(:,1).^2]; 
elseif (n==2)
    X=[fs(:,1),fs(:,2),fs(:,1).^2,fs(:,2).^2,fs(:,1).*fs(:,2)];
elseif (n==3)
    X=[fs(:,1),fs(:,2),fs(:,3),fs(:,1).^2,fs(:,2).^2,fs(:,3).^2,fs(:,1).*fs(:,2),fs(:,1).*fs(:,3),fs(:,2).*fs(:,3)];
elseif (n==4)
    X=[fs(:,1),fs(:,2),fs(:,3),fs(:,4),fs(:,1).^2,fs(:,2).^2,fs(:,3).^2,fs(:,4).^2,fs(:,1).*fs(:,2),fs(:,1).*fs(:,3),fs(:,1).*fs(:,4),fs(:,2).*fs(:,3),fs(:,2).*fs(:,4),fs(:,3).*fs(:,4)];
end
%profits
Y =fvals(:,5+n+1);
%fit model
mdl = fitlm(X,Y);
%coefficients
coefs = mdl.Coefficients.Estimate;
%Rsquared
r2 = mdl.Rsquared.Ordinary;
r2adj = mdl.Rsquared.Adjusted;


