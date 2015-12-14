function [coefs, r2, ps,fvals] = second_stage_2player(phi,m,beta,S,N)
options = optimset('Display', 'off') ;
tic
n = 2; 
ps=zeros(20^n,1+2*n+nchoosek(n,2)+ 6);
fvals=zeros(20^n,1+2*n+nchoosek(n,2)+ 6);
%symmetric S
S=ones(n,1)*S;
M=1000; C=ones(n,1)*10000;
p0=100;
eps=0.1;
c=0;
count=0;
%solve second stage game for all frequency combinations
f_combos  = permn(1:20,2);
for combo = 1:20^n
    f = f_combos(combo,:);
    p=ones(n,1)*p0;
    diff=ones(n,1)*(eps+1);
    fval=zeros(n,1);
    while sum(diffs)>eps
        for i=1:n
            p_i=p(i);
            fun_i=@(p_i)profit_nplayer_scheddelay(p,f,M,S,C,phi,m,beta,N,i,p_i);
            [x_i, fval(i)]=fmincon(fun_i,p0,[],[],[],[],0,[],[],options);
            diff(i)=abs(p(i)-x_i);
            p(i)=x_i;
        end
        c=c+1;
    end
    count=count+1;
    ps(count,:)=[phi,m,beta,S,N,f,p];
    fvals(count,:)=[phi,m,beta,S,N,f,fval];
    if (mod(count,1000)==0)
        display(count)
        toc
    end            
end
%